// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nolint:errcheck
package groupbytraceprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor"

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
)

type UInt64Set map[uint64]struct{}

// groupByTraceProcessor is a processor that keeps traces in memory for a given duration, with the expectation
// that the trace will be complete once this duration expires. After the duration, the trace is sent to the next consumer.
// This processor uses a buffered event machine, which converts operations into events for non-blocking processing, but
// keeping all operations serialized per worker scope. This ensures that we don't need locks but that the state is consistent across go routines.
// Initially, all incoming batches are split into different traces and distributed among workers by a hash of traceID in eventMachine.consume method.
// Afterwards, the trace is registered with a go routine, which will be called after the given duration and dispatched to the event
// machine for further processing.
// The typical data flow looks like this:
// ConsumeTraces -> eventMachine.consume(trace) -> event(traceReceived) -> onTraceReceived -> AfterFunc(duration, event(traceExpired)) -> onTraceExpired
// async markAsReleased -> event(traceReleased) -> onTraceReleased -> nextConsumer
// Each worker in the eventMachine also uses a ring buffer to hold the in-flight trace IDs, so that we don't hold more than the given maximum number
// of traces in memory/storage. Items that are evicted from the buffer are discarded without warning.
type groupByTraceProcessor struct {
	nextConsumer consumer.Traces
	config       Config
	logger       *zap.Logger
	traceUIDs    UInt64Set
	// the event machine handling all operations for this processor
	eventMachine *eventMachine

	// the trace storage
	st storage
}

var _ component.TracesProcessor = (*groupByTraceProcessor)(nil)

const bufferSize = 10_000

// newGroupByTraceProcessor returns a new processor.
func newGroupByTraceProcessor(logger *zap.Logger, st storage, nextConsumer consumer.Traces, config Config) *groupByTraceProcessor {
	// the event machine will buffer up to N concurrent events before blocking
	eventMachine := newEventMachine(logger, 10000, config.NumWorkers, config.NumTraces)

	traceUIDs := make(map[uint64]struct{})
	sp := &groupByTraceProcessor{
		logger:       logger,
		nextConsumer: nextConsumer,
		config:       config,
		eventMachine: eventMachine,
		st:           st,
		traceUIDs:    traceUIDs,
	}

	// register the callbacks
	eventMachine.onTraceReceived = sp.onTraceReceived
	eventMachine.onTraceExpired = sp.onTraceExpired
	eventMachine.onTraceReleased = sp.onTraceReleased
	eventMachine.onTraceRemoved = sp.onTraceRemoved

	return sp
}

func (sp *groupByTraceProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	var errs error
	for _, singleTrace := range batchpersignal.SplitTraces(td) {
		errs = multierr.Append(errs, sp.eventMachine.consume(singleTrace))
	}
	return errs
}

func (sp *groupByTraceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start is invoked during service startup.
func (sp *groupByTraceProcessor) Start(context.Context, component.Host) error {
	// start these metrics, as it might take a while for them to receive their first event
	stats.Record(context.Background(), mTracesEvicted.M(0))
	stats.Record(context.Background(), mDeDuplicatedTraces.M(0))
	stats.Record(context.Background(), mNumOfDistinctTraces.M(0))
	stats.Record(context.Background(), mIncompleteReleases.M(0))
	stats.Record(context.Background(), mNumTracesConf.M(int64(sp.config.NumTraces)))

	sp.eventMachine.startInBackground()
	return sp.st.start()
}

// Shutdown is invoked during service shutdown.
func (sp *groupByTraceProcessor) Shutdown(_ context.Context) error {
	sp.eventMachine.shutdown()
	return sp.st.shutdown()
}

func (sp *groupByTraceProcessor) onTraceReceived(trace tracesWithID, worker *eventMachineWorker) error {
	traceID := trace.id
	if worker.buffer.contains(traceID) {
		sp.logger.Debug("trace is already in memory storage")

		// it exists in memory already, just append the spans to the trace in the storage
		if err := sp.addSpans(traceID, trace.td); err != nil {
			return fmt.Errorf("couldn't add spans to existing trace: %w", err)
		}

		//Check if span links, if so, mark trace as sendable and the dest also
		// we are done with this trace, move on
		return nil
	}

	// at this point, we determined that we haven't seen the trace yet, so, record the
	// traceID in the map and the spans to the storage

	// place the trace ID in the buffer, and check if an item had to be evicted
	evicted := worker.buffer.put(traceID)
	if !evicted.IsEmpty() {
		// delete from the storage
		worker.fire(event{
			typ:     traceRemoved,
			payload: evicted,
		})

		stats.Record(context.Background(), mTracesEvicted.M(1))

		sp.logger.Info("trace evicted: in order to avoid this in the future, adjust the wait duration and/or number of traces to keep in memory",
			zap.String("traceID", evicted.HexString()))
	}

	// we have the traceID in the memory, place the spans in the storage too
	if err := sp.addSpans(traceID, trace.td); err != nil {
		return fmt.Errorf("couldn't add spans to existing trace: %w", err)
	}

	sp.logger.Debug("scheduled to release trace", zap.Duration("duration", sp.config.WaitDuration))

	time.AfterFunc(sp.config.WaitDuration, func() {
		// if the event machine has stopped, it will just discard the event
		worker.fire(event{
			typ:     traceExpired,
			payload: traceID,
		})
	})
	return nil
}

func (sp *groupByTraceProcessor) onTraceExpired(traceID pcommon.TraceID, worker *eventMachineWorker) error {
	sp.logger.Debug("processing expired", zap.String("traceID",
		traceID.HexString()))

	if !worker.buffer.contains(traceID) {
		// we likely received multiple batches with spans for the same trace
		// and released this trace already
		sp.logger.Debug("skipping the processing of expired trace",
			zap.String("traceID", traceID.HexString()))

		stats.Record(context.Background(), mIncompleteReleases.M(1))
		return nil
	}

	// delete from the map and erase its memory entry
	worker.buffer.delete(traceID)
	sp.st.setLinkedSpans(traceID)

	// this might block, but we don't need to wait
	sp.logger.Debug("marking the trace as released",
		zap.String("traceID", traceID.HexString()))
	go sp.markAsReleased(traceID, worker.fire)

	return nil
}

func (sp *groupByTraceProcessor) markAsReleased(traceID pcommon.TraceID, fire func(...event)) error {
	// #get is a potentially blocking operation
	trace, err := sp.st.get(traceID)
	if err != nil {
		return fmt.Errorf("couldn't retrieve trace %q from the storage: %w", traceID, err)
	}

	if trace == nil {
		return fmt.Errorf("the trace %q couldn't be found at the storage", traceID)
	}

	// signal that the trace is ready to be released
	sp.logger.Debug("trace marked as released", zap.String("traceID", traceID.HexString()))

	// atomically fire the two events, so that a concurrent shutdown won't leave
	// an orphaned trace in the storage
	shouldSend := sp.st.getSendState(traceID)
	fire(event{
		typ: traceReleased,
		// Traces
		payload: SomeStruct{trace: trace, tid: traceID, shouldSend: shouldSend},
	}, event{
		typ:     traceRemoved,
		payload: traceID,
	})
	return nil
}

func (sp *groupByTraceProcessor) generateTraceUID(trace ptrace.Traces) ([]uint64, bool) {
	vss := trace.ResourceSpans()
	trace_uids := make([]uint64, trace.SpanCount())
	var span_counter int = 0
	for i := 0; i < vss.Len(); i++ {
		rs := vss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			for g := 0; g < ilss.At(j).Spans().Len(); g++ {
				operation := ilss.At(j).Spans().At(g).Name()
				// var image_name internal.Value
				image_name, ok := rs.Resource().Attributes().Get("oxeye.image_name")
				if !ok {
					image_name, ok = ilss.At(j).Spans().At(g).Attributes().Get("oxeye.image_name")
					if !ok {
						return nil, false
					}
				}

				// namespace, ok := rs.Resource().Attributes().Get("oxeye.container_namespace")
				// namespace, ok := ilss.At(j).Spans().At(g).Attributes().Get("oxeye.container_namespace")
				if !ok {
					return nil, false
				}

				// observer_id, ok := rs.Resource().Attributes().Get("oxeye.observer_id")
				// observer_id, ok := ilss.At(j).Spans().At(g).Attributes().Get("oxeye.observer_id")
				if !ok {
					return nil, false
				}

				// account_id, ok := rs.Resource().Attributes().Get("oxeye.customer_id")
				// account_id, ok := ilss.At(j).Spans().At(g).Attributes().Get("oxeye.customer_id")
				if !ok {
					return nil, false
				}

				h := fnv.New64a()
				h.Write([]byte(operation))

				h.Write([]byte(image_name.StringVal()))
				// h.Write([]byte(namespace.StringVal()))
				// h.Write([]byte(account_id.StringVal()))
				// h.Write([]byte(observer_id.StringVal()))
				trace_uids[span_counter] = h.Sum64()
				span_counter++
			}
		}
	}
	return trace_uids, true
}

func (sp *groupByTraceProcessor) isDuplicate(trace ptrace.Traces) bool {
	trace_uids, ok := sp.generateTraceUID(trace)
	if !ok {
		return true
	}

	sort.Slice(trace_uids, func(i, j int) bool { return trace_uids[i] < trace_uids[j] })
	uid := make([]byte, 8*trace.SpanCount())

	for i := 0; i < len(trace_uids); i++ {
		binary.LittleEndian.PutUint64(uid[i*8:], trace_uids[i])
	}

	uid = uid[0:]
	h := fnv.New64()
	h.Write(uid)
	trace_uid := h.Sum64()

	if _, ok := sp.traceUIDs[trace_uid]; ok {
		stats.Record(context.Background(), mDeDuplicatedTraces.M(1))
		return true
	}

	stats.Record(context.Background(), mNumOfDistinctTraces.M(1))
	sp.traceUIDs[trace_uid] = struct{}{}
	return false
}

func (sp *groupByTraceProcessor) onTraceReleased(rss []ptrace.ResourceSpans, traceID pcommon.TraceID, shouldSend bool) error {
	trace := ptrace.NewTraces()
	for _, rs := range rss {
		trs := trace.ResourceSpans().AppendEmpty()
		rs.CopyTo(trs)
	}

	stats.Record(context.Background(),
		mReleasedSpans.M(int64(trace.SpanCount())),
		mReleasedTraces.M(1),
	)

	if shouldSend || !sp.isDuplicate(trace) {
		// Do async consuming not to block event worker
		go func() {
			if err := sp.nextConsumer.ConsumeTraces(context.Background(), trace); err != nil {
				sp.logger.Error("consume failed", zap.Error(err))
			}
		}()
	}
	return nil
}

func (sp *groupByTraceProcessor) onTraceRemoved(traceID pcommon.TraceID) error {
	trace, err := sp.st.delete(traceID)
	if err != nil {
		return fmt.Errorf("couldn't delete trace %q from the storage: %w", traceID.HexString(), err)
	}

	if trace == nil {
		return fmt.Errorf("trace %q not found at the storage", traceID.HexString())
	}

	return nil
}

func (sp *groupByTraceProcessor) addSpans(traceID pcommon.TraceID, trace ptrace.Traces) error {
	sp.logger.Debug("creating trace at the storage", zap.String("traceID", traceID.HexString()))
	return sp.st.createOrAppend(traceID, trace)
}

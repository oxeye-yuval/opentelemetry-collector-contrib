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

package groupbytraceprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor"

import (
	"context"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SendableTrace struct {
	trace        []ptrace.ResourceSpans
	linkedTraces []pcommon.TraceID
	shouldSend   bool
}
type memoryStorage struct {
	sync.RWMutex
	content                   map[pcommon.TraceID]SendableTrace
	stopped                   bool
	stoppedLock               sync.RWMutex
	metricsCollectionInterval time.Duration
}

var _ storage = (*memoryStorage)(nil)

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		content:                   make(map[pcommon.TraceID]SendableTrace),
		metricsCollectionInterval: time.Second,
	}
}

func (st *memoryStorage) createOrAppend(traceID pcommon.TraceID, td ptrace.Traces) error {
	st.Lock()
	defer st.Unlock()

	// getting zero value is fine
	content := st.content[traceID].trace
	var shouldBeSent = false

	newRss := ptrace.NewResourceSpansSlice()
	td.ResourceSpans().CopyTo(newRss)
	for i := 0; i < newRss.Len(); i++ {
		rss := newRss.At(i)
		for j := 0; j < rss.ScopeSpans().Len(); j++ {
			for k := 0; k < rss.ScopeSpans().At(i).Spans().Len(); k++ {
				if 0 < rss.ScopeSpans().At(i).Spans().At(k).Links().Len() {
					shouldBeSent = true
					for q := 0; q < rss.ScopeSpans().At(i).Spans().At(k).Links().Len(); q++ {
						if !st.setSendState(rss.ScopeSpans().At(i).Spans().At(k).Links().At(q).TraceID(), true) {
							cont := st.content[traceID]
							cont.linkedTraces = append(st.content[traceID].linkedTraces, rss.ScopeSpans().At(i).Spans().At(k).Links().At(q).TraceID())
							st.content[traceID] = cont
						}
					}
				}
			}
		}
		content = append(content, rss)
	}
	cont := st.content[traceID]
	cont.trace = content
	st.content[traceID] = cont

	if shouldBeSent {
		st.setSendState(traceID, true)
	}

	return nil
}
func (st *memoryStorage) get(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	st.RLock()
	sendable, ok := st.content[traceID]
	st.RUnlock()
	if !ok {
		return nil, nil
	}

	rss := sendable.trace
	var result []ptrace.ResourceSpans
	for _, rs := range rss {
		newRS := ptrace.NewResourceSpans()
		rs.CopyTo(newRS)
		result = append(result, newRS)
	}

	return result, nil
}

func (st *memoryStorage) setLinkedSpans(traceID pcommon.TraceID) error {
	st.RLock()
	defer st.RUnlock()
	sendable, ok := st.content[traceID]
	if !ok {
		return nil
	}

	for i := 0; i < len(sendable.linkedTraces); i++ {
		st.setSendState(sendable.linkedTraces[i], true)
	}
	return nil
}

func (st *memoryStorage) setSendState(traceID pcommon.TraceID, sendState bool) bool {
	// st.RLock()
	// defer st.RUnlock()
	sendable, ok := st.content[traceID]
	if !ok {
		return false
	}
	sendable.shouldSend = sendState
	st.content[traceID] = sendable
	return true
}

func (st *memoryStorage) getSendState(traceID pcommon.TraceID) bool {
	st.RLock()
	defer st.RUnlock()
	sendable, ok := st.content[traceID]
	if !ok {
		return false
	}
	return sendable.shouldSend
}

// delete will return a reference to a ResourceSpans. Changes to the returned object may not be applied
// to the version in the storage.
func (st *memoryStorage) delete(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	st.Lock()
	defer st.Unlock()

	defer delete(st.content, traceID)
	return st.content[traceID].trace, nil
}

func (st *memoryStorage) start() error {
	go st.periodicMetrics()
	return nil
}

func (st *memoryStorage) shutdown() error {
	st.stoppedLock.Lock()
	defer st.stoppedLock.Unlock()
	st.stopped = true
	return nil
}

func (st *memoryStorage) periodicMetrics() {
	numTraces := st.count()
	stats.Record(context.Background(), mNumTracesInMemory.M(int64(numTraces)))

	st.stoppedLock.RLock()
	stopped := st.stopped
	st.stoppedLock.RUnlock()
	if stopped {
		return
	}

	time.AfterFunc(st.metricsCollectionInterval, func() {
		st.periodicMetrics()
	})
}

func (st *memoryStorage) count() int {
	st.RLock()
	defer st.RUnlock()
	return len(st.content)
}

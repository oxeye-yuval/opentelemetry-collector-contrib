package groupbytraceprocessor

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/go-redis/cache/v9"
	"github.com/redis/go-redis/v9"

	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type redisStorage struct {
	sync.RWMutex
	logger                    *zap.Logger
	redisClient               *redis.Client
	redisCache                *cache.Cache
	stopped                   bool
	stoppedLock               sync.RWMutex
	metricsCollectionInterval time.Duration
}

var _ storage = (*redisStorage)(nil)

const redisPrefix string = "dedup-Trace:"

func newRedisStorage(logger *zap.Logger, redisClient *redis.Client) *redisStorage {
	redisCache := cache.New(
		&cache.Options{
			Redis: redisClient,
		})
	return &redisStorage{
		logger:                    logger,
		redisClient:               redisClient,
		redisCache:                redisCache,
		metricsCollectionInterval: time.Second,
	}
}

func (st *redisStorage) getRedisKey(traceID pcommon.TraceID) string {
	return redisPrefix + traceID.HexString()
}

func (st *redisStorage) getSendableTraceFromRedis(traceID pcommon.TraceID) (*SendableTrace, error) {
	st.logger.Debug("Getting Trace from redis", zap.String("traceID", traceID.HexString()))
	st.RLock()
	defer st.RUnlock()

	var sendableTrace SendableTrace
	var marshalableSendableTrace MarshalableSendableTrace

	err := st.redisCache.Get(context.Background(), st.getRedisKey(traceID), &marshalableSendableTrace)
	if errors.Is(err, cache.ErrCacheMiss) {
		sendableTrace = SendableTrace{
			Trace:        make([]ptrace.ResourceSpans, 0),
			LinkedTraces: make([]pcommon.TraceID, 0),
			ShouldSend:   false,
		}
	} else if err != nil {
		return nil, err
	} else {
		sendableTrace = *marshalableSendableTrace.ToSendableTrace(st.logger)
	}
	return &sendableTrace, nil
}

func (st *redisStorage) setSendableTraceInRedis(traceID pcommon.TraceID, sendableTrace SendableTrace) {
	st.logger.Debug("Setting Trace in redis", zap.String("traceID", traceID.HexString()))
	st.Lock()
	defer st.Unlock()

	marshalableSendableTrace := sendableTrace.ToMarshalableSendableTrace(st.logger)

	err := st.redisCache.Set(
		&cache.Item{
			Ctx:   context.Background(),
			Key:   st.getRedisKey(traceID),
			Value: &marshalableSendableTrace,
			TTL:   10 * time.Minute,
		})
	if err != nil {
		panic(err)
	}
}

func (st *redisStorage) deleteSendableTraceFromRedis(traceID pcommon.TraceID) {
	st.logger.Debug("Deleting Trace from redis", zap.String("traceID", traceID.HexString()))
	st.Lock()
	defer st.Unlock()
	_ = st.redisCache.Delete(context.Background(), st.getRedisKey(traceID))
}

func (st *redisStorage) countSendableTracesInRedis() int {
	st.RLock()
	defer st.RUnlock()

	var ctx = context.Background()
	var count = 0
	iter := st.redisClient.Scan(ctx, 0, redisPrefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		count++
	}
	return count
}

func (st *redisStorage) createOrAppend(traceID pcommon.TraceID, td ptrace.Traces) error {
	st.logger.Debug("CreateOrAppend", zap.String("traceID", traceID.HexString()))
	//st.Lock()
	//defer st.Unlock()

	sendableTrace, err := st.getSendableTraceFromRedis(traceID)
	if err != nil {
		panic(err)
	}
	var shouldBeSent = false

	newRss := ptrace.NewResourceSpansSlice()
	td.ResourceSpans().CopyTo(newRss)
	for i := 0; i < newRss.Len(); i++ {
		rss := newRss.At(i)
		for k := 0; k < rss.ScopeSpans().At(i).Spans().Len(); k++ {
			if 0 < rss.ScopeSpans().At(i).Spans().At(k).Links().Len() {
				shouldBeSent = true
				for q := 0; q < rss.ScopeSpans().At(i).Spans().At(k).Links().Len(); q++ {
					if !st.setSendState(rss.ScopeSpans().At(i).Spans().At(k).Links().At(q).TraceID(), true) {
						st.logger.Debug("Appending to sendableTrace.LinkedTraces")
						sendableTrace.LinkedTraces = append(
							sendableTrace.LinkedTraces,
							rss.ScopeSpans().At(i).Spans().At(k).Links().At(q).TraceID(),
						)
					}
				}
			}
		}
		st.logger.Debug("Appending to sendableTrace.Trace")
		sendableTrace.Trace = append(sendableTrace.Trace, rss)
	}

	if shouldBeSent {
		st.setSendState(traceID, true)
	}

	st.setSendableTraceInRedis(traceID, *sendableTrace)

	return nil
}

func (st *redisStorage) get(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	//st.RLock()
	sendable, err := st.getSendableTraceFromRedis(traceID)
	//st.RUnlock()
	if err != nil {
		return nil, nil
	}

	rss := sendable.Trace
	var result []ptrace.ResourceSpans
	for _, rs := range rss {
		newRS := ptrace.NewResourceSpans()
		rs.CopyTo(newRS)
		result = append(result, newRS)
	}

	return result, nil
}

func (st *redisStorage) setLinkedSpans(traceID pcommon.TraceID) error {
	//st.Lock()
	//defer st.Unlock()
	sendable, err := st.getSendableTraceFromRedis(traceID)
	if err != nil {
		return nil
	}

	for i := 0; i < len(sendable.LinkedTraces); i++ {
		st.setSendState(sendable.LinkedTraces[i], true)
	}
	return nil
}

func (st *redisStorage) setSendState(traceID pcommon.TraceID, sendState bool) bool {
	//st.Lock()
	//defer st.Unlock()
	sendable, err := st.getSendableTraceFromRedis(traceID)
	if err != nil {
		return false
	}
	sendable.ShouldSend = sendState
	st.setSendableTraceInRedis(traceID, *sendable)
	return true
}

func (st *redisStorage) getSendState(traceID pcommon.TraceID) bool {
	//st.RLock()
	//defer st.RUnlock()
	sendable, err := st.getSendableTraceFromRedis(traceID)
	if err != nil {
		return false
	}
	return sendable.ShouldSend
}

// delete will return a reference to a ResourceSpans. Changes to the returned object may not be applied
// to the version in the storage.
func (st *redisStorage) delete(traceID pcommon.TraceID) ([]ptrace.ResourceSpans, error) {
	//st.Lock()
	//defer st.Unlock()

	defer st.deleteSendableTraceFromRedis(traceID)

	sendableTrace, err := st.getSendableTraceFromRedis(traceID)
	if err != nil {
		return nil, err
	}
	return sendableTrace.Trace, nil
}

func (st *redisStorage) start() error {
	go st.periodicMetrics()
	return nil
}

func (st *redisStorage) shutdown() error {
	st.stoppedLock.Lock()
	defer st.stoppedLock.Unlock()
	st.stopped = true
	return nil
}

func (st *redisStorage) periodicMetrics() {
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

func (st *redisStorage) count() int {
	//st.RLock()
	//defer st.RUnlock()
	return st.countSendableTracesInRedis()
}

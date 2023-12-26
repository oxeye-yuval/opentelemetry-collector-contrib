package groupbytraceprocessor

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type SendableTrace struct {
	Trace        []ptrace.ResourceSpans
	LinkedTraces []pcommon.TraceID
	ShouldSend   bool
}

type MyResourceSpans struct {
	Resource   pcommon.Resource
	ScopeSpans ptrace.ScopeSpansSlice
	SchemaUrl  string
}

type MyLinkedTraces struct {
	Id [16]byte
}

type MarshalableSendableTrace struct {
	Trace        []byte
	LinkedTraces []MyLinkedTraces
	ShouldSend   bool
}

func (st *SendableTrace) ToMarshalableSendableTrace(logger *zap.Logger) *MarshalableSendableTrace {
	trace := ptrace.NewTraces()
	for _, rs := range st.Trace {
		trs := trace.ResourceSpans().AppendEmpty()
		rs.CopyTo(trs)
	}

	var myResourceSpans = make([]MyResourceSpans, 0)
	for _, t := range st.Trace {
		myResourceSpans = append(myResourceSpans, MyResourceSpans{
			Resource:   t.Resource(),
			ScopeSpans: t.ScopeSpans(),
			SchemaUrl:  t.SchemaUrl(),
		})
	}

	var myLinkedTraces = make([]MyLinkedTraces, 0)
	for _, lt := range st.LinkedTraces {
		myLinkedTraces = append(myLinkedTraces, MyLinkedTraces{
			Id: lt.Bytes(),
		})
	}

	jsonMarshaler := ptrace.NewJSONMarshaler()
	marshaledTrace, err := jsonMarshaler.MarshalTraces(trace)

	if err != nil {
		logger.Error("Could not marshal sendable trace", zap.Error(err))
		return nil
	}

	return &MarshalableSendableTrace{
		Trace:        marshaledTrace,
		LinkedTraces: myLinkedTraces,
		ShouldSend:   st.ShouldSend,
	}
}

func (mst *MarshalableSendableTrace) ToSendableTrace(logger *zap.Logger) *SendableTrace {
	var err error
	var trace ptrace.Traces
	jsonUnmarshaler := ptrace.NewJSONUnmarshaler()
	trace, err = jsonUnmarshaler.UnmarshalTraces(mst.Trace)

	if err != nil {
		logger.Error("Could not unmarshal sendable trace", zap.Error(err))
		return nil
	}

	var resourceSpans = make([]ptrace.ResourceSpans, 0)
	resourceSpansSlice := trace.ResourceSpans()
	for i := 0; i < resourceSpansSlice.Len(); i++ {
		resourceSpan := resourceSpansSlice.At(i)
		resourceSpans = append(resourceSpans, resourceSpan)
	}

	var linkedTraces = make([]pcommon.TraceID, 0)
	for _, lt := range mst.LinkedTraces {
		tid := pcommon.NewTraceID(lt.Id)
		linkedTraces = append(linkedTraces, tid)
	}

	return &SendableTrace{
		Trace:        resourceSpans,
		LinkedTraces: linkedTraces,
		ShouldSend:   mst.ShouldSend,
	}
}

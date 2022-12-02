package distkvs

import "github.com/DistributedClocks/tracing"

func receiveToken(token tracing.TracingToken, tracer *tracing.Tracer) *tracing.Trace {
	if token != nil && tracer != nil {
		return tracer.ReceiveToken(token)
	}
	return nil
}

func generateToken(trace *tracing.Trace) tracing.TracingToken {
	if trace != nil {
		return trace.GenerateToken()
	}
	return nil
}

func recordAction(trace *tracing.Trace, action interface{}) {
	if trace != nil {
		trace.RecordAction(action)
	}
}

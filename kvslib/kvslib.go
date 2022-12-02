// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net/rpc"
	"sync"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type RequestChannel chan RequestStruct

type RequestStruct struct {
	isPut    bool
	tracer   *tracing.Tracer
	clientId string
	key      string
	value    string
	opId     uint32
}

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type ResultStructToken struct {
	OpId        uint32
	StorageFail bool
	Result      *string
	Token       tracing.TracingToken
}

type CloseChannel chan struct{}

type GetArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

type PutArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type KVS struct {
	notifyCh    NotifyChannel
	frontend    *rpc.Client
	kvslibTrace *tracing.Trace
	clientId    string
	closeCh     CloseChannel
	closeWg     *sync.WaitGroup
	mu          sync.Mutex
	nextOpId    uint32
	requestCh   RequestChannel
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh:    nil,
		frontend:    nil,
		kvslibTrace: nil,
		clientId:    "",
		closeCh:     nil,
		closeWg:     nil,
		mu:          sync.Mutex{},
		nextOpId:    0,
		requestCh:   nil,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	frontend, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing coordinator: %s", err)
	}

	d.frontend = frontend
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.closeCh = make(CloseChannel, 1)
	d.requestCh = make(RequestChannel, chCapacity)

	d.kvslibTrace = d.createTrace(localTracer)
	d.recordAction(KvslibBegin{ClientId: clientId}, d.kvslibTrace)

	d.clientId = clientId

	d.mu = sync.Mutex{}
	d.closeWg = &sync.WaitGroup{}

	go func() {
		for {
			select {
			case req := <-d.requestCh:

				if req.isPut {
					d.callPut(req)
				} else {
					d.callGet(req)
				}
			case <-d.closeCh:
				return
			}
		}
	}()

	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	opId := d.getNextOpId()
	d.closeWg.Add(1)
	d.requestCh <- RequestStruct{false, tracer, clientId, key, "", opId}
	return opId, nil
}

func (d *KVS) callGet(request RequestStruct) {
	defer d.closeWg.Done()

	getTrace := d.createTrace(request.tracer)

	d.recordAction(KvslibGet{request.clientId, request.opId, request.key}, getTrace)

	token := d.generateToken(getTrace)

	args := GetArgs{request.clientId, request.opId, request.key, token}
	var result = ResultStructToken{}

	call := d.frontend.Go("FrontEnd.Get", args, &result, nil)
	for {
		select {
		case <-call.Done:
			d.receiveToken(request.tracer, result.Token)
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				d.recordAction(KvslibGetResult{OpId: request.opId, Key: request.key, Value: result.Result, Err: result.StorageFail}, getTrace)

				d.notifyCh <- ResultStruct{Result: result.Result, StorageFail: result.StorageFail, OpId: result.OpId}
			}
			return
		case <-d.closeCh:
			log.Printf("cancel callGet")
			return
		}
	}
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	opId := d.getNextOpId()

	d.closeWg.Add(1)

	d.requestCh <- RequestStruct{true, tracer, clientId, key, value, opId}
	return opId, nil
}

func (d *KVS) callPut(request RequestStruct) {
	defer d.closeWg.Done()

	putTrace := d.createTrace(request.tracer)

	d.recordAction(KvslibPut{request.clientId, request.opId, request.key, request.value}, putTrace)

	token := d.generateToken(putTrace)

	args := PutArgs{request.clientId, request.opId, request.key, request.value, token}
	var result = ResultStructToken{}

	call := d.frontend.Go("FrontEnd.Put", args, &result, nil)

	for {
		select {
		case <-call.Done:
			d.receiveToken(request.tracer, result.Token)
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				d.recordAction(KvslibPutResult{OpId: request.opId, Err: result.StorageFail}, putTrace)
				d.notifyCh <- ResultStruct{Result: result.Result, StorageFail: result.StorageFail, OpId: result.OpId}
			}
			return
		case <-d.closeCh:
			log.Printf("cancel callPut")
			return
		}
	}
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	d.closeCh <- struct{}{}
	d.closeWg.Wait()

	err := d.frontend.Close()
	if err != nil {
		return err
	}

	d.frontend = nil

	d.recordAction(KvslibComplete{d.clientId}, d.kvslibTrace)
	return nil
}

func (d *KVS) getNextOpId() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	opId := d.nextOpId
	d.nextOpId++
	return opId
}

func (d *KVS) createTrace(tracer *tracing.Tracer) *tracing.Trace {
	if tracer != nil {
		return tracer.CreateTrace()
	} else {
		return nil
	}
}

func (d *KVS) recordAction(record interface{}, trace *tracing.Trace) {
	if trace != nil {
		trace.RecordAction(record)
	}
}

func (d *KVS) generateToken(trace *tracing.Trace) tracing.TracingToken {
	if trace != nil {
		return trace.GenerateToken()
	} else {
		return nil
	}
}

func (d *KVS) receiveToken(tracer *tracing.Tracer, token tracing.TracingToken) {
	if tracer != nil {
		tracer.ReceiveToken(token)
	}
}

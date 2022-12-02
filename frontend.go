package distkvs

import (
	"example.org/cpsc416/a6/kvslib"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/DistributedClocks/tracing"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type FrontEndStorageJoined struct {
	StorageIds []string
}

type FrontEnd struct {
	tracer         *tracing.Tracer
	ftTrace        *tracing.Trace
	storageTimeout uint8

	StorageClientMap map[string]*StorageClient

	RequestLock sync.Mutex // to serialize requests, fk it
}

type StorageClient struct {
	client    *rpc.Client
	isStarted bool
	storageID string
}

type FrontEndGetArgs struct {
	Key       string
	ClientId  string
	RequestId uint32
	Token     tracing.TracingToken
	OpId      uint32
}

type FrontEndGetReply struct {
	Token tracing.TracingToken
	Value string
	Key   string
}

type FrontEndPutArgs struct {
	Key      string
	ClientId string
	OpId     uint32
	Token    tracing.TracingToken
	Value    string
}

type FrontEndPutReply struct {
	Token tracing.TracingToken
}

type FrontEndConnArgs struct {
	StorageListenAddress string
	StoragId             string
	Token                tracing.TracingToken
}

type FrontEndConnReply struct {
	Token   tracing.TracingToken
	IsFirst bool
}

type ResultStruct struct {
	Key         *string
	OpId        uint32
	StorageFail bool
	Result      *string
	ClientId    *string
	Token       tracing.TracingToken
	StorageID   string
}

func (f *FrontEnd) Get(args FrontEndGetArgs, reply *kvslib.ResultStructToken) error {

	f.RequestLock.Lock()
	defer f.RequestLock.Unlock()
	trace := receiveToken(args.Token, f.tracer)

	recordAction(trace, FrontEndGet{Key: args.Key})

	var results []*string
	for _, storageClient := range f.StorageClientMap {
		for tryCount := 0; tryCount < 2; tryCount++ {
			storageArgs := &StorageArgs{
				Key:      args.Key,
				Token:    nil,
				ClientId: args.ClientId,
				OpId:     args.OpId,
			}
			ret := new(GetResult)
			reply.StorageFail = false
			storageArgs.Token = generateToken(trace)

			call := storageClient.client.Go("Storage.Get", storageArgs, ret, nil)

			// made blocking for now, we can re-think this later if needed
			<-call.Done

			receiveToken(ret.Token, f.tracer)

			if call.Error != nil {
				if tryCount == 0 {
					// timeout -> reattempt
					time.Sleep(time.Second * time.Duration(f.storageTimeout))
					continue
				} else {
					f.setHasStorageFailed(storageClient.storageID)
				}
			} else {
				results = append(results, ret.Value)
				break
			}
		}
	}

	// Case when all storages failed
	if len(results) == 0 {
		recordAction(trace, FrontEndGetResult{
			Err:   true,
			Key:   args.Key,
			Value: nil})
		reply.Result = nil
		reply.StorageFail = true
		reply.Token = generateToken(trace)
		return nil
	}

	// Case when there was a mismatch between the results received
	value := results[0]
	for i := 1; i < len(results); i++ {
		if value == nil && results[i] != nil ||
			value != nil && results[i] == nil ||
			value != nil && results[i] != nil && *value != *results[i] {
			log.Println("the results received from storage were inconsistent ")
			recordAction(trace, FrontEndGetResult{
				Err:   true,
				Key:   args.Key,
				Value: nil})
			reply.Result = nil
			reply.StorageFail = true
			reply.Token = generateToken(trace)
			return nil
		}
	}

	// Success case
	recordAction(trace, FrontEndGetResult{
		Err:   false,
		Key:   args.Key,
		Value: value})
	reply.StorageFail = false
	reply.Result = value

	reply.Token = generateToken(trace)
	return nil

}

func (f *FrontEnd) Put(args FrontEndPutArgs, reply *kvslib.ResultStructToken) error {
	f.RequestLock.Lock()
	defer f.RequestLock.Unlock()

	trace := receiveToken(args.Token, f.tracer)

	recordAction(trace, FrontEndPut{
		Key:   args.Key,
		Value: args.Value,
	})

	isAtLeastOneSuccess := false
	for _, storageClient := range f.StorageClientMap {
		storageArgs := StorageArgs{
			Key:      args.Key,
			Value:    args.Value,
			Token:    nil,
			OpId:     args.OpId,
			ClientId: args.ClientId,
		}
		ret := new(PutResult)
		reply.StorageFail = false
		for tryCount := 0; tryCount < 2; tryCount++ {
			storageArgs.Token = generateToken(trace)
			call := storageClient.client.Go("Storage.Put", storageArgs, ret, nil)

			// made blocking for now, we can re-think this later if needed
			<-call.Done

			receiveToken(ret.Token, f.tracer)

			if call.Error != nil {
				if tryCount == 0 {
					// timeout -> reattempt
					time.Sleep(time.Second * time.Duration(f.storageTimeout))
					continue
				} else {
					f.setHasStorageFailed(storageClient.storageID)
				}
			} else {
				isAtLeastOneSuccess = true
				break
			}
		}
	}

	// Case when all storages failed
	if !isAtLeastOneSuccess {
		recordAction(trace, FrontEndPutResult{
			Err: true})
		reply.Result = nil
		reply.StorageFail = true
		reply.Token = generateToken(trace)
		return nil
	}

	recordAction(trace, FrontEndPutResult{Err: false})
	reply.StorageFail = false
	reply.Token = generateToken(trace)
	return nil
}

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	f.storageTimeout = storageTimeout
	f.tracer = ftrace
	f.ftTrace = ftrace.CreateTrace()

	f.StorageClientMap = make(map[string]*StorageClient)

	server := rpc.NewServer()
	err := server.Register(f)
	if err != nil {
		return fmt.Errorf("format of FrontEnd RPCs isn't correct: %s", err)
	}

	clientListener, e := net.Listen("tcp", clientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", clientAPIListenAddr, e)
	}

	storageListener, e := net.Listen("tcp", storageAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, e)
	}

	go server.Accept(storageListener)
	server.Accept(clientListener)

	return nil
}

func (f *FrontEnd) ConnectStorage(args FrontEndConnArgs, reply *FrontEndConnReply) error {
	f.RequestLock.Lock()
	defer f.RequestLock.Unlock()

	trace := receiveToken(args.Token, f.tracer)
	trace.RecordAction(FrontEndStorageStarted{StorageID: args.StoragId})
	storage, err := rpc.Dial("tcp", args.StorageListenAddress)
	if err != nil {
		return err
	}

	f.StorageClientMap[args.StoragId] = &StorageClient{client: storage, storageID: args.StoragId, isStarted: false}

	if len(f.StorageClientMap) == 1 {
		ids := []string{args.StoragId}
		recordAction(trace, FrontEndStorageJoined{ids})
		reply.Token = generateToken(trace)
		reply.IsFirst = true
		return nil
	}

	if len(f.StorageClientMap) > 1 {
		storageGetDataArgs := StorageGetDataArgs{}
		storageGetDataReply := StorageGetDataReply{}
		f.StorageClientMap[args.StoragId].isStarted = false

		for id, val := range f.StorageClientMap {
			if id != args.StoragId {
				storageGetDataArgs.Token = generateToken(trace)
				err := val.client.Call("Storage.GetData", storageGetDataArgs, &storageGetDataReply)
				receiveToken(storageGetDataReply.Token, f.tracer)
				// successful call so we break
				if err == nil {
					break
				}
			}
		}

		storageReceiveData := StorageReceiveDataArgs{
			Token: generateToken(trace),
			Data:  storageGetDataReply.Data,
		}
		storageReceiveReply := StorageReceiveDataReply{}

		call := f.StorageClientMap[args.StoragId].client.Go("Storage.ReceiveData", storageReceiveData, &storageReceiveReply, nil)
		<-call.Done
		receiveToken(storageReceiveReply.Token, f.tracer)
		if call.Error != nil {
			delete(f.StorageClientMap, args.StoragId)
			return err
		} else {
			f.logStorageJoined()
		}
	}

	// Only the first storage in the system can start right away, others must join first
	reply.Token = generateToken(trace)
	reply.IsFirst = false
	return nil
}

func (f *FrontEnd) logStorageJoined() {
	ids := []string{}
	for key, _ := range f.StorageClientMap {
		ids = append(ids, key)
	}
	recordAction(f.ftTrace, FrontEndStorageJoined{StorageIds: ids})
}

func (f *FrontEnd) setHasStorageFailed(storageID string) {
	if _, ok := f.StorageClientMap[storageID]; ok {
		// Only need to record FrontEndStorageFailed once for a single failure
		// Alternates with FrontEndStorageStarted
		// happens before  FrontEndPutResult{Err: true} and  FrontEndGetResult{Err: true}
		delete(f.StorageClientMap, storageID)
		recordAction(f.ftTrace, FrontEndStorageFailed{storageID})

		f.logStorageJoined()
	}
}

func (f *FrontEnd) generateTaskKey(clientId string, opId uint32) string {
	return fmt.Sprintf("%s|%d", clientId, opId)
}

package distkvs

import (
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	StorageID string
	State     map[string]string
}

type StoragePut struct {
	StorageID string
	Key       string
	Value     string
}

type StorageSaveData struct {
	StorageID string
	Key       string
	Value     string
}

type StorageGet struct {
	StorageID string
	Key       string
}

type StorageGetResult struct {
	StorageID string
	Key       string
	Value     *string
}

type StorageJoining struct {
	StorageID string
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

type Storage struct {
	// state may go here
	KeyValStore    KeyValueStore
	FrontEndClient *rpc.Client
	DiskPath       string
	Trace          *tracing.Trace
	Tracer         *tracing.Tracer

	RequestMap map[string]bool
	PutWg      *sync.WaitGroup
	StorageId  string
}

type StorageConnectArgs struct {
	Addr string
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	server := rpc.NewServer()
	err := server.Register(s)
	if err != nil {
		return fmt.Errorf("failed registering storage RPCs: %s", err)
	}

	frontendListener, e := net.Listen("tcp", storageAddr)
	if e != nil {
		log.Fatalf("failed to listen on %s: %s", storageAddr, e)
	}

	go server.Accept(frontendListener)

	frontend, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return fmt.Errorf("failed to dial frontend: %s", err)
	}

	s.FrontEndClient = frontend
	s.KeyValStore.Initialize(storageId)
	s.DiskPath = diskPath + "/" + storageId + "/"
	s.Trace = strace.CreateTrace()
	s.RequestMap = make(map[string]bool)
	s.StorageId = storageId
	s.Tracer = strace

	err = s.load()
	if err != nil {
		log.Fatal("error loading")
	}

	recordAction(s.Trace, StorageLoadSuccess{State: s.KeyValStore.Store, StorageID: storageId})
	recordAction(s.Trace, StorageJoining{storageId})

	s.catchUp(storageAddr)

	// Only the first storage in the system may start right away, others must join and catch up first

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

	return nil
}

func (s *Storage) catchUp(storageAddr string) {
	frontendConnReply := &FrontEndConnReply{}
	frontendConnArgs := &FrontEndConnArgs{
		StorageListenAddress: storageAddr,
		StoragId:             s.StorageId,
		Token:                generateToken(s.Trace),
	}
	call := s.FrontEndClient.Go("FrontEnd.ConnectStorage", frontendConnArgs, frontendConnReply, nil)
	<-call.Done
	s.Tracer.ReceiveToken(frontendConnReply.Token)
	if call.Error != nil {
		return
	}

	if frontendConnReply.IsFirst {
		recordAction(s.Trace, StorageJoined{StorageID: s.StorageId, State: s.KeyValStore.Store})
	}
}

type StorageArgs struct {
	Key      string
	Value    string
	Token    tracing.TracingToken
	ClientId string
	OpId     uint32
}

type StorageGetDataArgs struct {
	Token                tracing.TracingToken
	StorageListenAddress string
}

type StorageGetDataReply struct {
	StorageListenAddress string
	Token                tracing.TracingToken
	Data                 map[string]string
}

type StorageReceiveDataArgs struct {
	Token tracing.TracingToken
	Data  map[string]string
}

type StorageReceiveDataReply struct {
	Token tracing.TracingToken
	Data  map[string]string
}

type PutResult struct {
	Token tracing.TracingToken
}

type GetResult struct {
	Value *string
	Token tracing.TracingToken
}

func (s *Storage) Put(args StorageArgs, result *PutResult) error {
	trace := receiveToken(args.Token, s.Tracer)

	recordAction(trace, StoragePut{
		StorageID: s.StorageId,
		Key:       args.Key,
		Value:     args.Value,
	})

	s.save(trace, args.Key, args.Value)

	result.Token = generateToken(trace)
	return nil
}

func (s *Storage) Get(args StorageArgs, result *GetResult) error {
	trace := receiveToken(args.Token, s.Tracer)

	recordAction(trace, StorageGet{Key: args.Key, StorageID: s.StorageId})

	result.Value = nil
	if val, ok := s.KeyValStore.get(args, trace); ok {
		result.Value = &val
	} else {
		result.Value = nil
	}

	result.Token = generateToken(trace)
	return nil
}

func (s *Storage) GetData(args StorageGetDataArgs, reply *StorageGetDataReply) error {
	trace := receiveToken(args.Token, s.Tracer)
	log.Println("getting data asss")
	reply.Data = s.KeyValStore.Store

	reply.Token = generateToken(trace)
	return nil
}

func (s *Storage) ReceiveData(args StorageReceiveDataArgs, reply *StorageReceiveDataReply) error {
	trace := receiveToken(args.Token, s.Tracer)

	s.KeyValStore.Store = args.Data

	recordAction(trace, StorageJoined{StorageID: s.StorageId, State: s.KeyValStore.Store})

	reply.Token = generateToken(trace)
	return nil
}

func (s *Storage) load() error {
	exists, err := exists(s.DiskPath)
	if err != nil {
		log.Fatal("failed")
	}
	if !exists {
		err := os.MkdirAll(s.DiskPath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}
	items, _ := ioutil.ReadDir(s.DiskPath)
	for _, item := range items {
		data, err := ioutil.ReadFile(s.DiskPath + item.Name())
		if err != nil {
			log.Fatal(err)
		}
		var val string
		err = json.Unmarshal(data, &val)
		if err != nil {
			return err
		}
		s.KeyValStore.put(item.Name(), val)
	}
	return nil
}

//An optimization could be if we write new states to disk appending to a line
//Then on load what we do is start at the bottom and if we see repeat keys we just use the newest value for that key as we iterate through the list of state
func (s *Storage) save(trace *tracing.Trace, key string, value string) {
	if s.KeyValStore.put(key, value) {
		storeJson, err := json.Marshal(value)
		if err != nil {
			log.Fatal(err)
		}
		err = ioutil.WriteFile(s.DiskPath+key, storeJson, 0644)
		if err != nil {
			log.Fatal(err)
		}
		recordAction(trace, StorageSaveData{
			Key:       key,
			Value:     value,
			StorageID: s.StorageId,
		})
	}
}

// Unmarshal is a function that unmarshals the data from the
// reader into the specified value.
// By default, it uses the JSON unmarshaller.
var Unmarshal = func(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

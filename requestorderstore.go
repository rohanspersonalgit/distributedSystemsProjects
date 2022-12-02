package distkvs

import (
	"log"
	"sync"
)

type RequestOrderStorage struct {
	CurrentRequest uint
	NumberLock     sync.Mutex
	RequestMap     map[uint]chan struct{}
	ReadLock       sync.Mutex
}

func NewRequestOrderStorage() *RequestOrderStorage{
	ret := RequestOrderStorage{}
	ret.ReadLock = sync.Mutex{}
	ret.NumberLock = sync.Mutex{}
	ret.CurrentRequest = 1
	ret.RequestMap = make(map[uint]chan struct{})
	return &ret
}

func (r *RequestOrderStorage) IncrementRequest(){
	r.NumberLock.Lock()
	defer r.NumberLock.Unlock()
	r.CurrentRequest += 1
	return
}

func (r *RequestOrderStorage) getRequestNumber() uint{
	r.NumberLock.Lock()
	defer r.NumberLock.Unlock()
	return r.CurrentRequest
}

// blocks if out of order
func (r *RequestOrderStorage) CheckOrder(requestNum uint){
	log.Println(requestNum)
	log.Print(r.getRequestNumber())
	r.NumberLock.Lock()
	if requestNum != r.CurrentRequest{
		r.NumberLock.Unlock()
		r.ReadLock.Lock()
		log.Print("thisar")
		r.RequestMap[requestNum-1] = make(chan struct{}, 1)
		r.ReadLock.Unlock()
		<-r.RequestMap[requestNum-1]
		delete(r.RequestMap,requestNum-1)
	}else{
		r.NumberLock.Unlock()
	}
}

func (r *RequestOrderStorage) ReleaseRequestChan(requestNum uint){
	r.ReadLock.Lock()
	defer r.ReadLock.Unlock()
	if checkVal, ok := r.RequestMap[requestNum]; ok{
		checkVal<- struct{}{}
	}
}
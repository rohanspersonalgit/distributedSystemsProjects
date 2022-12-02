package distkvs

import (
	"github.com/DistributedClocks/tracing"
)

type KeyValueStore struct {
	Store     map[string]string
	StorageId string
}

func (k *KeyValueStore) Initialize(storageId string) {
	k.Store = make(map[string]string)
	k.StorageId = storageId
}

//return true if value updated, false if not updated
func (k *KeyValueStore) put(key string, value string) bool {
	if checkVal, ok := k.Store[key]; ok {
		if checkVal == value {
			return true
		}
	}
	k.Store[key] = value
	return true
}
func (k *KeyValueStore) get(args StorageArgs, trace *tracing.Trace) (string, bool) {
	if checkVal, ok := k.Store[args.Key]; ok {
		recordAction(trace, StorageGetResult{
			Key:       args.Key,
			Value:     &checkVal,
			StorageID: k.StorageId,
		})
		return checkVal, true
	}
	recordAction(trace, StorageGetResult{
		Key:       args.Key,
		Value:     nil,
		StorageID: k.StorageId,
	})
	return "", false
}

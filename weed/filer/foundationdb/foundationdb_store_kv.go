//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
)

func (store *FoundationDB) KvPut(ctx context.Context, key []byte, value []byte) error {

	err := store.putKV(key, value)
	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}
	return nil
}

func (store *FoundationDB) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	resp, err := store.getKV(key)
	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	if resp == nil {
		return nil, filer.ErrKvNotFound
	}

	return resp, nil
}

func (store *FoundationDB) KvDelete(ctx context.Context, key []byte) (err error) {

	err = store.deleteKV(key)

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}

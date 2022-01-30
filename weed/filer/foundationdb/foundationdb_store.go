//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
)

type FoundationDB struct {
	db fdb.Database
}

const (
	fdbRequiredAPIVersion = 620
)

func init() {
	filer.Stores = append(filer.Stores, &FoundationDB{})
}

func (store *FoundationDB) GetName() string {
	return "foundationdb"
}

func (store *FoundationDB) Initialize(configuration weed_util.Configuration, prefix string) error {
	clusterFile := configuration.GetString(prefix + "clusterfile")
	if clusterFile == "" {
		glog.V(0).Infof("Using default cluster")
	}

	fdb.MustAPIVersion(fdbRequiredAPIVersion)
	var err error
	store.db, err = fdb.OpenDatabase(clusterFile)
	if err != nil {
		return fmt.Errorf("connect to foundationdb err:%s", err)
	}
	return nil
}

func (store *FoundationDB) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *FoundationDB) CommitTransaction(ctx context.Context) error {
	return nil
}

func (store *FoundationDB) RollbackTransaction(ctx context.Context) error {
	return nil
}

//func genKey(dirPath, fileName string) (key []byte) {
//	key = []byte(dirPath)
//	key = append(key, DIR_FILE_SEPARATOR)
//	key = append(key, []byte(fileName)...)
//	return key
//}

func (store *FoundationDB) putKV(key fdb.Key, value []byte) error {
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// TODO: do i need to split to chunks ? what are the values here ???
		tr.Set(key, value)
		return nil, nil
	})
	return err
}

func (store *FoundationDB) getKV(key fdb.Key) ([]byte, error) {
	resp, err := store.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		return tr.Get(key).Get()
	})

	return resp.([]byte), err
}

func (store *FoundationDB) deleteKV(key fdb.Key) error {
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(key)
		return nil, nil
	})
	return err
}

func (store *FoundationDB) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	//key := genKey(entry.DirAndName())
	//path := entry.FullPath
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.Chunks) > 50 {
		meta = weed_util.MaybeGzipData(meta)
	}

	err = store.putKV(fdb.Key(entry.FullPath), meta)
	if err != nil {
		return fmt.Errorf("insert %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	return nil
}

func (store *FoundationDB) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *FoundationDB) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	resp, err := store.getKV(fdb.Key(fullpath))
	if err != nil {
		return nil, fmt.Errorf("findentry %s: %v", fullpath, err)
	}

	if resp == nil {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(resp))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *FoundationDB) DeleteEntry(ctx context.Context, path weed_util.FullPath) error {
	err := store.deleteKV(fdb.Key(path))
	if err != nil {
		return fmt.Errorf("delete %s : %v", path, err)
	}
	return nil
}

func (store *FoundationDB) DeleteFolderChildren(ctx context.Context, path weed_util.FullPath) error {
	prefix, err := fdb.PrefixRange([]byte(path))
	if err != nil {
		return fmt.Errorf("deletefolder prefix %s : %v", path, err)
	}

	_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(prefix)
		return nil, nil
	})

	if err != nil {
		return fmt.Errorf("deletefolder  %s : %v", path, err)
	}

	return nil
}

func (store *FoundationDB) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *FoundationDB) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	var firstEntry string
	if startFileName != "" {
		firstEntry = fmt.Sprintf("%s/%s", dirPath, startFileName)
	} else {
		firstEntry = string(dirPath)
	}

	prefixBytes := append([]byte(dirPath), 0xFF)
	fdbRange := fdb.KeyRange{Begin: fdb.Key(firstEntry), End: fdb.Key(prefixBytes)}
	var startIndex = 0
	if !includeStartFile {
		startIndex = 1
		limit = limit + 1
	}

	result, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.GetRange(fdbRange, fdb.RangeOptions{Limit: int(limit)}).GetSliceWithError()
	})

	if err != nil {
		return lastFileName, fmt.Errorf("read %s : %v", dirPath, err)
	}

	kvResult := result.([]fdb.KeyValue)
	if len(kvResult) == 0 {
		return lastFileName, nil
	}

	//var lastPath weed_util.FullPath
	var lastEntry weed_util.FullPath
	for _, kvEntry := range kvResult[startIndex:] {
		lastEntry = weed_util.FullPath(kvEntry.Key)
		entry := &filer.Entry{
			FullPath: lastEntry,
		}

		if err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(kvEntry.Value)); err != nil {
			glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
			return lastFileName, fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
		}

		if !eachEntryFunc(entry) {
			break
		}
	}

	return lastEntry.Name(), nil
}

func (store *FoundationDB) Shutdown() {
	// NOOP
}

package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDBStorage ...
type LevelDBStorage struct {
	db *leveldb.DB
}

/// NewLevelDBStorage ...
func NewLevelDBStorage(path string) (storage *LevelDBStorage, err error) {
	var db *leveldb.DB
	db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		return
	}
	return &LevelDBStorage{
		db: db,
	}, nil
}

// Get ...
func (storage *LevelDBStorage) Get(key string) (value string, err error) {
	var data []byte
	data, err = storage.db.Get([]byte(key), nil)
	if err != nil {
		return
	}
	value = string(data)
	return
}

// Set ...
func (storage *LevelDBStorage) Set(key string, value string) error {
	return storage.db.Put([]byte(key), []byte(value), &opt.WriteOptions{
		NoWriteMerge: true,
		Sync:         true,
	})
}

// GetAllKeys ...
func (storage *LevelDBStorage) GetAllKeys() (keys []string, err error) {
	iter := storage.db.NewIterator(nil, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		keys = append(keys, string(key))
	}
	iter.Release()
	err = iter.Error()
	return
}

// GetAll ...
func (storage *LevelDBStorage) GetAll() (values map[string]string, err error) {
	values = make(map[string]string)
	iter := storage.db.NewIterator(nil, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		values[string(key)] = string(value)
	}
	iter.Release()
	err = iter.Error()
	return
}

// GetAllKeysByPrefix ...
func (storage *LevelDBStorage) GetAllKeysByPrefix(prefix string) (keys []string, err error) {
	iter := storage.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		keys = append(keys, string(key))
	}
	iter.Release()
	err = iter.Error()
	return
}

// GetAllByPrefix ...
func (storage *LevelDBStorage) GetAllByPrefix(prefix string) (values map[string]string, err error) {
	values = make(map[string]string)
	iter := storage.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		values[string(key)] = string(value)
	}
	iter.Release()
	err = iter.Error()
	return
}

// Delete ...
func (storage *LevelDBStorage) Delete(key string) error {
	return storage.db.Delete([]byte(key), nil)
}

// Close ...
func (storage *LevelDBStorage) Close() (err error) {
	err = storage.db.Close()
	return
}

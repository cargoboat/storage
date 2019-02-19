package storage

import "testing"

func TestLevelDBStorage(t *testing.T) {
	var store Storageer
	var err error
	store, err = NewLevelDBStorage("./cargoboat.db")
	if err != nil {
		t.Fatal(err)
	}
	testKey := "testkey"
	err = store.Set(testKey, "testvalue")
	if err != nil {
		t.Fatal(err)
	}

	var value string
	value, err = store.Get(testKey)
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("get value:%s", value)
	}

	var allValue map[string]string
	allValue, err = store.GetAll()
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("get allValue:%s", allValue)
	}
	allValue, err = store.GetAllByPrefix("test")
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("get GetAllByPrefix:%s", allValue)
	}

	err = store.Delete(testKey)
	if err != nil {
		t.Fatal(err)
	}
	var allKeys []string
	allKeys, err = store.GetAllKeys()
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("get allKeys:%s", allKeys)
	}

	err = store.Close()
	if err != nil {
		t.Fatal(err)
	}
}

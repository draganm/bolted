package database

import "github.com/draganm/bolted/dbpath"

type Bolted interface {
	BeginWrite() (WriteTx, error)
	BeginRead() (ReadTx, error)
	Close() error
	Observe(path dbpath.Matcher) (<-chan ObservedChanges, func())
}

type WriteTx interface {
	CreateMap(path dbpath.Path) error
	Delete(path dbpath.Path) error
	Put(path dbpath.Path, value []byte) error
	Rollback() error
	ReadTx
}

type ReadTx interface {
	Get(path dbpath.Path) ([]byte, error)
	Iterator(path dbpath.Path) (Iterator, error)
	Exists(path dbpath.Path) (bool, error)
	IsMap(path dbpath.Path) (bool, error)
	Size(path dbpath.Path) (uint64, error)
	Finish() error
}

type Iterator interface {
	GetKey() (string, error)
	GetValue() ([]byte, error)
	IsDone() (bool, error)
	Prev() error
	Next() error
	Seek(key string) error
	First() error
	Last() error
}

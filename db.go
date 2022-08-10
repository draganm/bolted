package bolted

import (
	"errors"

	"github.com/draganm/bolted/dbpath"
	"go.etcd.io/bbolt"
)

type Database interface {
	BeginWrite() (WriteTx, error)
	BeginRead() (ReadTx, error)
	Observe(path dbpath.Matcher) (<-chan ObservedChanges, func())
	Close() error
	Stats() (*bbolt.Stats, error)
}

type WriteTx interface {
	CreateMap(path dbpath.Path) error
	Delete(path dbpath.Path) error
	Put(path dbpath.Path, value []byte) error
	SetFillPercent(float64) error
	Rollback() error
	ReadTx
}

type ReadTx interface {
	Get(path dbpath.Path) ([]byte, error)
	Iterator(path dbpath.Path) (Iterator, error)
	Exists(path dbpath.Path) (bool, error)
	IsMap(path dbpath.Path) (bool, error)
	Size(path dbpath.Path) (uint64, error)
	ID() (uint64, error)
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

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrNotFound)
}

var ErrConflict = errors.New("conflict")

func IsConflict(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrConflict)
}

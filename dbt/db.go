package dbt

import (
	"errors"
	"io"

	"github.com/draganm/bolted/dbpath"
	"go.etcd.io/bbolt"
)

type Database interface {
	Write(func(tx WriteTx) error) error
	Read(func(tx ReadTx) error) error

	Observe(path dbpath.Matcher) (<-chan ObservedChanges, func())
	Close() error
	Stats() (*bbolt.Stats, error)
}

type WriteTx interface {
	CreateMap(path dbpath.Path)
	Delete(path dbpath.Path)
	Put(path dbpath.Path, value []byte)
	SetFillPercent(float64)
	ReadTx
}

type ReadTx interface {
	Get(path dbpath.Path) []byte
	Iterator(path dbpath.Path) Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
	ID() uint64
	Dump(w io.Writer) (n int64)
	FileSize() int64
}

type Iterator interface {
	GetKey() string
	GetValue() []byte
	IsDone() bool
	Prev()
	Next()
	Seek(key string)
	First()
	Last()
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

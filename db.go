package bolted

import (
	"context"
	"errors"
	"io"

	"github.com/draganm/bolted/dbpath"
	"go.etcd.io/bbolt"
)

type Database interface {
	Read(func(tx ReadTx) error) error
	ReadWithContext(context.Context, func(tx ReadTx) error) error

	Write(func(tx WriteTx) error) error
	WriteWithContext(context.Context, func(tx WriteTx) error) error

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
	Iterate(path dbpath.Path) Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	GetSizeOf(path dbpath.Path) uint64
	ID() uint64
	DumpDatabase(w io.Writer) (n int64)
	GetDBFileSize() int64
	Context() context.Context
}

type Iterator interface {
	GetKey() string
	GetValue() []byte
	GetRawValue() []byte
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

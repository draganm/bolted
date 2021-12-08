package bolted

import (
	"errors"

	"github.com/draganm/bolted/dbpath"
	bolt "go.etcd.io/bbolt"
)

type Write interface {
	CreateMap(path dbpath.Path)
	Delete(path dbpath.Path)
	Put(path dbpath.Path, value []byte)
	Read
}

type Read interface {
	Get(path dbpath.Path) []byte
	Iterator(path dbpath.Path) *Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
}

type write struct {
	wtx WriteTx
}

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrNotFound)
}

func panicIfError(f func(path dbpath.Path) error, path dbpath.Path) {
	err := f(path)
	if err != nil {
		panic(err)
	}
}

func (w *write) CreateMap(path dbpath.Path) {
	panicIfError(w.wtx.CreateMap, path)
}

func (w *write) Delete(path dbpath.Path) {
	panicIfError(w.wtx.Delete, path)
}

func (w *write) Put(path dbpath.Path, value []byte) {
	err := w.wtx.Put(path, value)
	if err != nil {
		panic(err)
	}
}

func (w *write) Get(path dbpath.Path) []byte {
	res, err := w.wtx.Get(path)
	if err != nil {
		panic(err)
	}
	return res
}

type Iterator struct {
	c     *bolt.Cursor
	Key   string
	Value []byte
	Done  bool
}

func (i *Iterator) Next() {
	var k, v []byte
	k, v = i.c.Next()
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (i *Iterator) Prev() {
	var k, v []byte
	k, v = i.c.Prev()
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (i *Iterator) Seek(key string) {
	k, v := i.c.Seek([]byte(key))
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (i *Iterator) First() {
	k, v := i.c.First()
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (i *Iterator) Last() {
	k, v := i.c.Last()
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (w *write) Iterator(path dbpath.Path) *Iterator {
	it, err := w.wtx.Iterator(path)
	if err != nil {
		panic(err)
	}
	return it
}

func (w *write) Exists(path dbpath.Path) bool {
	ex, err := w.wtx.Exists(path)
	if err != nil {
		panic(err)
	}
	return ex
}

func (w *write) IsMap(path dbpath.Path) bool {
	ex, err := w.wtx.IsMap(path)
	if err != nil {
		panic(err)
	}
	return ex
}

func (w *write) Size(path dbpath.Path) uint64 {
	sz, err := w.wtx.Size(path)
	if err != nil {
		panic(err)
	}
	return sz
}

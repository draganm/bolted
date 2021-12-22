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
	Iterator(path dbpath.Path) Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
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

type iterator struct {
	c     *bolt.Cursor
	key   string
	value []byte
	done  bool
}

func (i *iterator) GetKey() string {
	return i.key
}

func (i *iterator) GetValue() []byte {
	return i.value
}

func (i *iterator) IsDone() bool {
	return i.done
}

func (i *iterator) Next() {
	var k, v []byte
	k, v = i.c.Next()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Prev() {
	var k, v []byte
	k, v = i.c.Prev()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Seek(key string) {
	k, v := i.c.Seek([]byte(key))
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) First() {
	k, v := i.c.First()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Last() {
	k, v := i.c.Last()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (w *write) Iterator(path dbpath.Path) Iterator {
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

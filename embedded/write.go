package embedded

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
	GetKey() (string, error)
	GetValue() ([]byte, error)
	IsDone() (bool, error)
	Prev() error
	Next() error
	Seek(key string) error
	First() error
	Last() error
	Close() error
}

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrNotFound)
}

type iterator struct {
	c     *bolt.Cursor
	key   string
	value []byte
	done  bool
}

func (i *iterator) Close() error {
	return nil
}

func (i *iterator) GetKey() (string, error) {
	return i.key, nil
}

func (i *iterator) GetValue() ([]byte, error) {
	return i.value, nil
}

func (i *iterator) IsDone() (bool, error) {
	return i.done, nil
}

func (i *iterator) Next() error {
	var k, v []byte
	k, v = i.c.Next()
	i.key = string(k)
	i.value = v
	i.done = k == nil

	return nil
}

func (i *iterator) Prev() error {
	var k, v []byte
	k, v = i.c.Prev()
	i.key = string(k)
	i.value = v
	i.done = k == nil
	return nil
}

func (i *iterator) Seek(key string) error {
	k, v := i.c.Seek([]byte(key))
	i.key = string(k)
	i.value = v
	i.done = k == nil
	return nil
}

func (i *iterator) First() error {
	k, v := i.c.First()
	i.key = string(k)
	i.value = v
	i.done = k == nil
	return nil
}

func (i *iterator) Last() error {
	k, v := i.c.Last()
	i.key = string(k)
	i.value = v
	i.done = k == nil
	return nil
}

package embedded

import (
	bolt "go.etcd.io/bbolt"
)

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

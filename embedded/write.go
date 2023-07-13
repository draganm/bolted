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

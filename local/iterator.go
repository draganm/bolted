package local

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

type iterator struct {
	c     *bolt.Cursor
	key   string
	value []byte
	done  bool
	ctx   context.Context
}

func (i iterator) checkForCancelledContext() {
	if i.ctx.Err() != nil {
		panic(i.ctx.Err())
	}
}

func (i *iterator) GetKey() string {
	i.checkForCancelledContext()
	return i.key
}

func (i *iterator) GetValue() []byte {
	i.checkForCancelledContext()
	return i.value
}

func (i *iterator) HasNext() bool {
	i.checkForCancelledContext()
	return !i.done
}

func (i *iterator) Next() {
	i.checkForCancelledContext()
	var k, v []byte
	k, v = i.c.Next()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Prev() {
	i.checkForCancelledContext()
	var k, v []byte
	k, v = i.c.Prev()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Seek(key string) {
	i.checkForCancelledContext()
	k, v := i.c.Seek([]byte(key))
	i.key = string(k)
	i.value = v
	i.done = k == nil

}

func (i *iterator) First() {
	i.checkForCancelledContext()
	k, v := i.c.First()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

func (i *iterator) Last() {
	i.checkForCancelledContext()
	k, v := i.c.Last()
	i.key = string(k)
	i.value = v
	i.done = k == nil
}

package bolted

import (
	"errors"

	"github.com/draganm/bolted/dbpath"
	bolt "go.etcd.io/bbolt"
)

type writeTx struct {
	btx             *bolt.Tx
	changeListeners CompositeChangeListener
	readOnly        bool
}

var ErrNotFound = errors.New("not found")

func (w *writeTx) CreateMap(path dbpath.Path) {

	if len(path) == 0 {
		panic(errors.New("root map already exists"))
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	_, err := bucket.CreateBucket([]byte(last))

	if err != nil {
		panic(err)
	}

	if !w.readOnly {
		err = w.changeListeners.CreateMap(w, path)
		if err != nil {
			panic(err)
		}
	}

}

func (w *writeTx) Delete(path dbpath.Path) {

	if len(path) == 0 {
		panic(errors.New("root cannot be deleted"))
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := []byte(path[len(path)-1])

	val := bucket.Get(last)
	if val != nil {
		err := bucket.Delete(last)
		if err != nil {
			panic(err)
		}

		if !w.readOnly {
			err = w.changeListeners.Delete(w, path)
			if err != nil {
				panic(err)
			}

		}
		return
	}

	b := bucket.Bucket(last)
	if b == nil {
		panic(ErrNotFound)
	}

	err := bucket.DeleteBucket(last)

	if err != nil {
		panic(err)
	}

	if !w.readOnly {
		err = w.changeListeners.Delete(w, path)
		if err != nil {
			panic(err)
		}
	}

}

func (w *writeTx) Put(path dbpath.Path, value []byte) {

	if len(path) == 0 {
		panic(errors.New("root cannot be deleted"))
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	err := bucket.Put([]byte(last), value)
	if err != nil {
		panic(err)
	}

	if !w.readOnly {
		err = w.changeListeners.Put(w, path, value)
		if err != nil {
			panic(err)
		}
	}

}

func (w *writeTx) Get(path dbpath.Path) []byte {

	if len(path) == 0 {
		panic(errors.New("cannot get value of root"))
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v == nil {
		panic(errors.New("value not found"))
	}

	return v

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

func (w *writeTx) Iterator(path dbpath.Path) *Iterator {

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	c := bucket.Cursor()
	k, v := c.First()

	return &Iterator{
		c:     c,
		Key:   string(k),
		Value: v,
		Done:  k == nil,
	}

}

func (w *writeTx) Exists(path dbpath.Path) bool {

	if len(path) == 0 {
		// root always exists
		return true
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return true
	}

	return bucket.Bucket([]byte(last)) != nil

}

func (w *writeTx) IsMap(path dbpath.Path) bool {

	if len(path) == 0 {
		// root is always a map
		return true
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return false
	}

	return bucket.Bucket([]byte(last)) != nil

}

func (w *writeTx) Size(path dbpath.Path) uint64 {

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		panic(errors.New("root bucket not found"))
	}

	if len(path) == 0 {
		return uint64(bucket.Stats().KeyN)
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			panic(errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return uint64(len(v))
	}

	bucket = bucket.Bucket([]byte(last))

	if bucket == nil {
		panic(errors.New("does not exist"))
	}

	return uint64(bucket.Stats().KeyN)

}

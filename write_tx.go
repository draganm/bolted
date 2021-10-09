package bolted

import (
	"errors"
	"fmt"

	"github.com/draganm/bolted/dbpath"
	bolt "go.etcd.io/bbolt"
)

type writeTx struct {
	btx             *bolt.Tx
	changeListeners CompositeChangeListener
	readOnly        bool
}

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrNotFound)
}

func (w *writeTx) CreateMap(path dbpath.Path) {
	err := w.createMap(path)
	if err != nil {
		panic(fmt.Errorf("CreateMap(%s): %w", path.String(), err))
	}
}

func (w *writeTx) createMap(path dbpath.Path) error {

	if len(path) == 0 {
		return errors.New("root map already exists")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	_, err := bucket.CreateBucket([]byte(last))

	if err != nil {
		return err
	}

	if !w.readOnly {
		err = w.changeListeners.CreateMap(w, path)
		if err != nil {
			return err
		}
	}

	return nil

}

func (w *writeTx) Delete(path dbpath.Path) {
	err := w.delete(path)
	if err != nil {
		panic(fmt.Errorf("Delete(%s): %w", path.String(), err))
	}
}

func (w *writeTx) delete(path dbpath.Path) error {

	if len(path) == 0 {
		return errors.New("root cannot be deleted")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := []byte(path[len(path)-1])

	val := bucket.Get(last)
	if val != nil {
		err := bucket.Delete(last)
		if err != nil {
			return err
		}

		if !w.readOnly {
			err = w.changeListeners.Delete(w, path)
			if err != nil {
				return err
			}

		}
		return nil
	}

	b := bucket.Bucket(last)
	if b == nil {
		return ErrNotFound
	}

	err := bucket.DeleteBucket(last)

	if err != nil {
		return err
	}

	if !w.readOnly {
		err = w.changeListeners.Delete(w, path)
		if err != nil {
			return err
		}
	}

	return nil

}

func (w *writeTx) Put(path dbpath.Path, value []byte) {
	err := w.put(path, value)
	if err != nil {
		panic(fmt.Errorf("Put(%s): %w", path.String(), err))
	}
}

func (w *writeTx) put(path dbpath.Path, value []byte) error {

	if len(path) == 0 {
		return errors.New("root cannot be deleted")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	err := bucket.Put([]byte(last), value)
	if err != nil {
		return err
	}

	if !w.readOnly {
		err = w.changeListeners.Put(w, path, value)
		if err != nil {
			return err
		}
	}

	return nil

}

func (w *writeTx) Get(path dbpath.Path) []byte {
	res, err := w.get(path)
	if err != nil {
		panic(fmt.Errorf("Get(%s): %w", path.String(), err))
	}
	return res
}

func (w *writeTx) get(path dbpath.Path) ([]byte, error) {

	if len(path) == 0 {
		return nil, errors.New("cannot get value of root")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return nil, errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return nil, errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v == nil {
		return nil, errors.New("value not found")
	}

	return v, nil

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
	it, err := w.iterator(path)
	if err != nil {
		panic(fmt.Errorf("Iterator(%s): %w", path.String(), err))
	}
	return it
}

func (w *writeTx) iterator(path dbpath.Path) (*Iterator, error) {

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return nil, errors.New("root bucket not found")
	}

	for _, p := range path {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return nil, errors.New("one of the parent buckets does not exist")
		}
	}

	c := bucket.Cursor()
	k, v := c.First()

	return &Iterator{
		c:     c,
		Key:   string(k),
		Value: v,
		Done:  k == nil,
	}, nil
}

func (w *writeTx) Exists(path dbpath.Path) bool {
	ex, err := w.exists(path)
	if err != nil {
		panic(fmt.Errorf("Exists(%s): %w", path.String(), err))
	}
	return ex
}

func (w *writeTx) exists(path dbpath.Path) (bool, error) {

	if len(path) == 0 {
		// root always exists
		return true, nil
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return false, errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return false, errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return true, nil
	}

	return bucket.Bucket([]byte(last)) != nil, nil

}

func (w *writeTx) IsMap(path dbpath.Path) bool {
	ex, err := w.isMap(path)
	if err != nil {
		panic(fmt.Errorf("IsMap(%s): %w", path.String(), err))
	}
	return ex
}

func (w *writeTx) isMap(path dbpath.Path) (bool, error) {

	if len(path) == 0 {
		// root is always a map
		return true, nil
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return false, errors.New("root bucket not found")
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return false, errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return false, nil
	}

	return bucket.Bucket([]byte(last)) != nil, nil

}

func (w *writeTx) Size(path dbpath.Path) uint64 {
	sz, err := w.size(path)
	if err != nil {
		panic(fmt.Errorf("Size(%s): %w", path.String(), err))
	}
	return sz
}

func (w *writeTx) size(path dbpath.Path) (uint64, error) {

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return 0, errors.New("root bucket not found")
	}

	if len(path) == 0 {
		return uint64(bucket.Stats().KeyN), nil
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return 0, errors.New("one of the parent buckets does not exist")
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return uint64(len(v)), nil
	}

	bucket = bucket.Bucket([]byte(last))

	if bucket == nil {
		return 0, errors.New("does not exist")
	}

	return uint64(bucket.Stats().KeyN), nil

}

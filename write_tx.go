package bolted

import (
	"errors"

	"github.com/draganm/bolted/dbpath"
	bolt "go.etcd.io/bbolt"
)

type writeTx struct {
	btx             *bolt.Tx
	changeListeners CompositeChangeListener
}

var ErrNotFound = errors.New("not found")

func (w *writeTx) CreateMap(path string) error {

	// TODO: create an uniform error layer

	parts, err := dbpath.Split(path)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		return errors.New("root map already exists")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := parts[len(parts)-1]

	_, err = bucket.CreateBucket([]byte(last))

	if err != nil {
		return err
	}

	return w.changeListeners.CreateMap(w, path)
}

func (w *writeTx) Delete(path string) error {

	// TODO: create an uniform error layer

	parts, err := dbpath.Split(path)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		return errors.New("root cannot be deleted")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := []byte(parts[len(parts)-1])

	val := bucket.Get(last)
	if val != nil {
		return bucket.Delete(last)
	}

	b := bucket.Bucket(last)
	if b == nil {
		return ErrNotFound
	}

	err = bucket.DeleteBucket(last)

	if err != nil {
		return err
	}

	return w.changeListeners.Delete(w, path)

}

func (w *writeTx) Put(path string, value []byte) error {

	// TODO: create an uniform error layer

	parts, err := dbpath.Split(path)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		return errors.New("root cannot be deleted")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return errors.New("root bucket not found")
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return errors.New("one of the parent buckets does not exist")
		}
	}

	last := parts[len(parts)-1]

	err = bucket.Put([]byte(last), value)
	if err != nil {
		return err
	}

	return w.changeListeners.Put(w, path, value)

}

func (w *writeTx) Get(path string) ([]byte, error) {

	// TODO: create an uniform error layer

	parts, err := dbpath.Split(path)
	if err != nil {
		return nil, err
	}

	if len(parts) == 0 {
		return nil, errors.New("cannot get value of root")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return nil, errors.New("root bucket not found")
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return nil, errors.New("one of the parent buckets does not exist")
		}
	}

	last := parts[len(parts)-1]

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
	k, v := i.c.Next()
	i.Key = string(k)
	i.Value = v
	i.Done = k == nil
}

func (w *writeTx) Iterator(path string, first string) (*Iterator, error) {

	parts, err := dbpath.Split(path)
	if err != nil {
		return nil, err
	}
	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return nil, errors.New("root bucket not found")
	}

	for _, p := range parts {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return nil, errors.New("one of the parent buckets does not exist")
		}
	}

	c := bucket.Cursor()
	k, v := c.Seek([]byte(first))

	return &Iterator{
		c:     c,
		Key:   string(k),
		Value: v,
		Done:  k == nil,
	}, nil

}

func (w *writeTx) Exists(path string) (bool, error) {
	parts, err := dbpath.Split(path)
	if err != nil {
		return false, err
	}

	if len(parts) == 0 {
		return false, errors.New("cannot get value of root")
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return false, errors.New("root bucket not found")
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return false, errors.New("one of the parent buckets does not exist")
		}
	}

	last := parts[len(parts)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return true, nil
	}

	return bucket.Bucket([]byte(last)) != nil, nil

}

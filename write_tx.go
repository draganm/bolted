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

	if !w.readOnly {
		return w.changeListeners.CreateMap(w, dbpath.Join(parts...))
	}

	return nil
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
		err = bucket.Delete(last)
		if err != nil {
			return err
		}

		if !w.readOnly {
			return w.changeListeners.Delete(w, dbpath.Join(parts...))
		}
	}

	b := bucket.Bucket(last)
	if b == nil {
		return ErrNotFound
	}

	err = bucket.DeleteBucket(last)

	if err != nil {
		return err
	}

	if !w.readOnly {
		return w.changeListeners.Delete(w, dbpath.Join(parts...))
	}

	return nil

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

	if !w.readOnly {
		return w.changeListeners.Put(w, dbpath.Join(parts...), value)
	}

	return nil

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

func (w *writeTx) Iterator(path string) (*Iterator, error) {

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
	k, v := c.First()

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
		// root always exists
		return true, nil
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

func (w *writeTx) IsMap(path string) (bool, error) {
	parts, err := dbpath.Split(path)
	if err != nil {
		return false, err
	}

	if len(parts) == 0 {
		// root is always a map
		return true, nil
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
		return false, nil
	}

	return bucket.Bucket([]byte(last)) != nil, nil

}

func (w *writeTx) Size(path string) (uint64, error) {
	parts, err := dbpath.Split(path)
	if err != nil {
		return 0, err
	}

	var bucket = w.btx.Bucket([]byte(rootBucketName))

	if bucket == nil {
		return 0, errors.New("root bucket not found")
	}

	if len(parts) == 0 {
		return uint64(bucket.Stats().KeyN), nil
	}

	for _, p := range parts[:len(parts)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return 0, errors.New("one of the parent buckets does not exist")
		}
	}

	last := parts[len(parts)-1]

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

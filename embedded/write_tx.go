package embedded

import (
	"errors"
	"fmt"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	bolt "go.etcd.io/bbolt"
)

type writeTx struct {
	btx        *bolt.Tx
	readOnly   bool
	rolledBack bool
}

func (w *writeTx) Finish() (err error) {
	if w.readOnly {
		w.btx.Rollback()
		return nil
	}

	if w.rolledBack {
		return nil
	}

	err = w.btx.Commit()

	if err != nil {
		return fmt.Errorf("while committing transaction: %w", err)
	}

	return nil
}

func (w *writeTx) Rollback() (err error) {
	if w.readOnly {
		return nil
	}

	err = w.btx.Rollback()
	if err != nil {
		return fmt.Errorf("while rolling back transaction: %w", err)
	}

	return nil
}

func (w *writeTx) CreateMap(path dbpath.Path) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("CreateMap(%s): %w", path.String(), err)
		}
	}()

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

	_, err = bucket.CreateBucket([]byte(last))

	if err != nil {
		return err
	}

	return nil

}

func (w *writeTx) Delete(path dbpath.Path) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("Delete(%s): %w", path.String(), err)
		}
	}()

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

		return nil
	}

	b := bucket.Bucket(last)
	if b == nil {
		return bolted.ErrNotFound
	}

	err = bucket.DeleteBucket(last)

	if err != nil {
		return err
	}

	return nil

}

func (w *writeTx) Put(path dbpath.Path, value []byte) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("Put(%s): %w", path.String(), err)
		}
	}()

	if len(path) == 0 {
		return errors.New("value cannot be put as root")
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

	err = bucket.Put([]byte(last), value)

	if err == bolt.ErrIncompatibleValue {
		return bolted.ErrConflict
	}

	if err != nil {
		return err
	}

	return nil

}

func (w *writeTx) Get(path dbpath.Path) (v []byte, err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("Get(%s): %w", path.String(), err)
		}
	}()

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

	v = bucket.Get([]byte(last))

	if v == nil {
		return nil, errors.New("value not found")
	}

	return v, nil

}

func (w *writeTx) Iterator(path dbpath.Path) (it bolted.Iterator, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("Iterator(%s): %w", path.String(), err)
		}
	}()

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

	return &iterator{
		c:     c,
		key:   string(k),
		value: v,
		done:  k == nil,
	}, nil
}

func (w *writeTx) Exists(path dbpath.Path) (ex bool, err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("Exists(%s): %w", path.String(), err)
		}
	}()

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

func (w *writeTx) IsMap(path dbpath.Path) (ism bool, err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("IsMap(%s): %w", path.String(), err)
		}
	}()

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

func (w *writeTx) Size(path dbpath.Path) (s uint64, err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("Size(%s): %w", path.String(), err)
		}
	}()

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

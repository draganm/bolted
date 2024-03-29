package bolted

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"

	"github.com/draganm/bolted/dbpath"
	"go.etcd.io/bbolt"
)

type writeTx struct {
	btx         *bbolt.Tx
	readOnly    bool
	rootBucket  *bbolt.Bucket
	fillPercent float64
	observer    *txObserver
	ctx         context.Context
}

func (w *writeTx) checkForCancelledContext() {
	if w.ctx.Err() != nil {
		panic(w.ctx.Err())
	}
}

func (w *writeTx) SetFillPercent(fillPercent float64) {
	w.checkForCancelledContext()
	if fillPercent < 0.1 {
		panic(fmt.Errorf("%s: %w", "SetFillPercent", errors.New("fill percent is too low")))
	}

	if fillPercent > 1.0 {
		panic(fmt.Errorf("%s: %w", "SetFillPercent", errors.New("fill percent is too high")))
	}
	w.fillPercent = fillPercent
}

func raiseErrorForPath(pth dbpath.Path, method string, err error) {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown.go"
		line = 0
	}
	file = filepath.Base(file)
	panic(fmt.Errorf("caller %s:%d: %s(%s): %w", file, line, method, pth.String(), err))
}

func (w *writeTx) CreateMap(path dbpath.Path) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		raiseErrorForPath(path, "CreateMap", errors.New("root map already exists"))
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "CreateMap", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "CreateMap", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	bucket.FillPercent = w.fillPercent

	_, err := bucket.CreateBucket([]byte(last))

	if err != nil {
		raiseErrorForPath(path, "CreateMap", err)
	}

	bucket.NextSequence()

	w.observer.createMap(path)

}

func (w *writeTx) Delete(path dbpath.Path) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		raiseErrorForPath(path, "Delete", errors.New("root cannot be deleted"))
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "Delete", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "Delete", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := []byte(path[len(path)-1])

	bucket.FillPercent = w.fillPercent

	countDownSize := func() {
		size := bucket.Sequence()
		if size == 0 {
			raiseErrorForPath(path, "Delete", errors.New("successful deletion from empty sequence - this should never happen"))
		}
		size--
		bucket.SetSequence(size)

	}

	val := bucket.Get(last)
	if val != nil {
		err := bucket.Delete(last)
		if err != nil {
			raiseErrorForPath(path, "Delete", err)
		}
		countDownSize()
		w.observer.delete(path)
		return
	}

	b := bucket.Bucket(last)
	if b == nil {
		raiseErrorForPath(path, "Delete", ErrNotFound)
	}

	err := bucket.DeleteBucket(last)

	if err != nil {
		raiseErrorForPath(path, "Delete", err)
	}

	countDownSize()

	w.observer.delete(path)

}

func (w *writeTx) Put(path dbpath.Path, value []byte) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		raiseErrorForPath(path, "Put", errors.New("value cannot be put as root"))
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "Put", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "Put", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	exists := bucket.Get([]byte(last)) != nil

	bucket.FillPercent = w.fillPercent

	err := bucket.Put([]byte(last), value)

	if err == bbolt.ErrIncompatibleValue {
		raiseErrorForPath(path, "Put", ErrConflict)
	}

	if err != nil {
		raiseErrorForPath(path, "Put", err)
	}

	if !exists {
		bucket.NextSequence()
	}

	w.observer.put(path)

}

func (w *writeTx) Get(path dbpath.Path) (v []byte) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		raiseErrorForPath(path, "Get", errors.New("cannot get value of root"))
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "Get", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "Get", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v = bucket.Get([]byte(last))

	if v == nil {
		raiseErrorForPath(path, "Get", errors.New("value not found"))
	}

	copyOfValue := make([]byte, len(v))
	copy(copyOfValue, v)

	return copyOfValue

}

func (w *writeTx) ID() uint64 {
	return uint64(w.btx.ID())
}

func (w *writeTx) Iterate(path dbpath.Path) (it Iterator) {
	w.checkForCancelledContext()

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "Iterate", errors.New("root bucket not found"))
	}

	for _, p := range path {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "Iterate", errors.New("one of the parent buckets does not exist"))
		}
	}

	c := bucket.Cursor()
	k, v := c.First()

	copyOfValue := make([]byte, len(v))
	copy(copyOfValue, v)

	return &iterator{
		c:     c,
		key:   string(k),
		value: copyOfValue,
		done:  k == nil,
		ctx:   w.ctx,
	}
}

func (w *writeTx) Exists(path dbpath.Path) (ex bool) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		// root always exists
		return true
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "Exists", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			return false
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return true
	}

	return bucket.Bucket([]byte(last)) != nil

}

func (w *writeTx) IsMap(path dbpath.Path) (ism bool) {
	w.checkForCancelledContext()

	if len(path) == 0 {
		// root is always a map
		return true
	}

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "IsMap", errors.New("root bucket not found"))
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "IsMap", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return false
	}

	return bucket.Bucket([]byte(last)) != nil

}

func (w *writeTx) GetSizeOf(path dbpath.Path) (s uint64) {
	w.checkForCancelledContext()

	var bucket = w.rootBucket

	if bucket == nil {
		raiseErrorForPath(path, "GetSizeOf", errors.New("root bucket not found"))
	}

	if len(path) == 0 {
		return bucket.Sequence()
	}

	for _, p := range path[:len(path)-1] {
		bucket = bucket.Bucket([]byte(p))
		if bucket == nil {
			raiseErrorForPath(path, "GetSizeOf", errors.New("one of the parent buckets does not exist"))
		}
	}

	last := path[len(path)-1]

	v := bucket.Get([]byte(last))

	if v != nil {
		return uint64(len(v))
	}

	bucket = bucket.Bucket([]byte(last))

	if bucket == nil {
		raiseErrorForPath(path, "GetSizeOf", errors.New("does not exist"))
	}

	return bucket.Sequence()

}

func (w *writeTx) DumpDatabase(wr io.Writer) (n int64) {
	w.checkForCancelledContext()
	n, err := w.btx.WriteTo(wr)
	if err != nil {
		panic(fmt.Errorf("%s: %w", "Dump", err))
	}
	return n
}

func (w *writeTx) GetDBFileSize() int64 {
	w.checkForCancelledContext()
	return w.btx.Size()
}

func (w *writeTx) Context() context.Context {
	return w.ctx
}

package bolted

import (
	"fmt"
	"os"

	"github.com/draganm/bolted/dbpath"

	bolt "go.etcd.io/bbolt"
)

type Bolted struct {
	db              *bolt.DB
	changeListeners CompositeChangeListener
	obs             *observer
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode, options ...Option) (*Bolted, error) {
	db, err := bolt.Open(path, mode, &bolt.Options{})
	if err != nil {
		return nil, fmt.Errorf("while opening bolt db: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(rootBucketName))
		if b == nil {
			_, err := tx.CreateBucket([]byte(rootBucketName))
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while creating root bucket: %w", err)
	}

	obs := newObserver()

	b := &Bolted{
		db:              db,
		changeListeners: CompositeChangeListener{obs},
		obs:             obs,
	}

	for _, o := range options {
		err = o(b)
		if err != nil {
			return nil, fmt.Errorf("while applying option: %w", err)
		}
	}

	err = b.changeListeners.Opened(b)
	if err != nil {
		return nil, fmt.Errorf("while handling Added by one of the change listeners: %w", err)
	}

	return b, nil

}

func (b *Bolted) Close() error {
	err := b.db.Close()
	if err != nil {
		return err
	}
	return b.changeListeners.Closed()
}

type Write interface {
	CreateMap(path dbpath.Path)
	Delete(path dbpath.Path)
	Put(path dbpath.Path, value []byte)
	Read
}

type Read interface {
	Get(path dbpath.Path) []byte
	Iterator(path dbpath.Path) *Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
}

func (b *Bolted) Write(f func(tx Write) error) error {
	err := b.db.Update(func(btx *bolt.Tx) (err error) {
		defer func() {
			e := recover()
			if e == nil {
				return
			}
			var isError bool
			err, isError = e.(error)
			if !isError {
				panic(e)
			}
		}()

		wtx := &writeTx{
			btx:             btx,
			changeListeners: b.changeListeners,
		}

		err = b.changeListeners.Start(wtx)
		if err != nil {
			return err
		}

		err = f(wtx)
		if err != nil {
			return err
		}

		err = b.changeListeners.BeforeCommit(wtx)
		if err != nil {
			return err
		}

		return nil
	})

	err2 := b.changeListeners.AfterTransaction(err)
	if err2 != nil {
		return err2
	}

	return err
}

func (b *Bolted) Read(f func(tx Read) error) error {
	return b.db.View(func(btx *bolt.Tx) (err error) {

		defer func() {
			e := recover()
			if e == nil {
				return
			}
			var isError bool
			err, isError = e.(error)
			if !isError {
				panic(e)
			}
		}()

		wtx := &writeTx{
			btx: btx,
		}
		return f(wtx)
	})
}

func (b *Bolted) Observe(path dbpath.Matcher) (<-chan ObservedChanges, func()) {
	ev, cl := b.obs.observe(path)
	return ev, cl
}

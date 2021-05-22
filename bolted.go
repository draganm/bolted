package bolted

import (
	"os"

	"github.com/draganm/bolted/dbpath"
	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "while opening bolt db")
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
		return nil, errors.Wrap(err, "while creating root bucket")
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
			return nil, errors.Wrap(err, "while applying option")
		}
	}

	err = b.changeListeners.Opened(b)
	if err != nil {
		return nil, errors.Wrap(err, "while handling Added by one of the change listeners")
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

type WriteTx interface {
	CreateMap(path dbpath.Path)
	Delete(path dbpath.Path)
	Put(path dbpath.Path, value []byte)
	ReadTx
}

type ReadTx interface {
	Get(path dbpath.Path) []byte
	Iterator(path dbpath.Path) *Iterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
}

func (b *Bolted) Write(f func(tx WriteTx) error) error {
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

func (b *Bolted) Read(f func(tx ReadTx) error) error {
	return b.db.View(func(btx *bolt.Tx) error {
		wtx := &writeTx{
			btx: btx,
		}
		return f(wtx)
	})
}

func (b *Bolted) Observe(path dbpath.Matcher) (chan ObservedChanges, func()) {
	ev, cl := b.obs.observe(path)
	return ev, cl
}

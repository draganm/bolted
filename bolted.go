package bolted

import (
	"os"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Bolted struct {
	db              *bolt.DB
	changeListeners CompositeChangeListener
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

	b := &Bolted{db: db}

	for _, o := range options {
		err = o(b)
		if err != nil {
			return nil, errors.Wrap(err, "while applying option")
		}
	}

	err = b.changeListeners.Added(b)
	if err != nil {
		return nil, errors.Wrap(err, "while handling Added by one of the change listeners")
	}

	return b, nil

}

func (b *Bolted) Close() error {
	return b.db.Close()
}

type WriteTx interface {
	CreateMap(path string) error
	Delete(path string) error
	Put(path string, value []byte) error
	ReadTx
}

type ReadTx interface {
	Get(path string) ([]byte, error)
	Iterator(path string) (*Iterator, error)
	Exists(path string) (bool, error)
}

func (b *Bolted) Write(f func(tx WriteTx) error) error {
	err := b.db.Update(func(btx *bolt.Tx) error {
		wtx := &writeTx{
			btx:             btx,
			changeListeners: b.changeListeners,
		}

		err := b.changeListeners.Start(wtx)
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

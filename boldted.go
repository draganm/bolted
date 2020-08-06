package bolted

import (
	"os"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Bolted struct {
	db *bolt.DB
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode) (*Bolted, error) {
	db, err := bolt.Open(path, mode, nil)
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

	return &Bolted{db: db}, nil

}

func (b *Bolted) Close() error {
	return b.db.Close()
}

type WriteTx interface {
	CreateMap(path string) error
	DeleteMap(path string) error
	Put(path string, value []byte) error
	ReadTx
}

type ReadTx interface {
	Get(path string) ([]byte, error)
	Iterator(path string, first string) (*Iterator, error)
	Exists(path string) (bool, error)
}

func (b *Bolted) Write(f func(tx WriteTx) error) error {
	return b.db.Update(func(btx *bolt.Tx) error {
		wtx := &writeTx{
			btx: btx,
		}
		return f(wtx)
	})
}

func (b *Bolted) Read(f func(tx ReadTx) error) error {
	return b.db.View(func(btx *bolt.Tx) error {
		wtx := &writeTx{
			btx: btx,
		}
		return f(wtx)
	})
}

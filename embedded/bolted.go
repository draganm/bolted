package embedded

import (
	"fmt"
	"io"
	"os"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"

	"go.etcd.io/bbolt"
)

type Bolted struct {
	db                *bbolt.DB
	obs               *observer
	writeTxDecorators []WriteTxDecorator
}

type Options struct {
	bbolt.Options
	WriteDecorators []WriteTxDecorator
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode, options Options) (*Bolted, error) {
	db, err := bbolt.Open(path, mode, &options.Options)
	if err != nil {
		return nil, fmt.Errorf("while opening bolt db: %w", err)
	}

	{
		tx, err := db.Begin(false)
		if err != nil {
			return nil, fmt.Errorf("while opening read tx: %w", err)
		}

		rootExists := tx.Bucket([]byte(rootBucketName)) != nil

		err = tx.Rollback()
		if err != nil {
			return nil, fmt.Errorf("while rolling back read transaction: %w", err)
		}

		if !rootExists {
			err = db.Update(func(tx *bbolt.Tx) error {
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
		}

	}

	obs := newObserver()

	b := &Bolted{
		db:                db,
		obs:               obs,
		writeTxDecorators: []WriteTxDecorator{obs.writeTxDecorator},
	}

	b.writeTxDecorators = append(b.writeTxDecorators, options.WriteDecorators...)

	return b, nil

}

func (b *Bolted) Close() error {
	err := b.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (b *Bolted) Dump(w io.Writer) (n int64, err error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, fmt.Errorf("while starting dump tx: %w", err)
	}

	defer tx.Rollback()

	return tx.WriteTo(w)

}

func (b *Bolted) Stats() (*bbolt.Stats, error) {
	st := b.db.Stats()
	return &st, nil
}

func (b *Bolted) BeginWrite() (bolted.WriteTx, error) {
	btx, err := b.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("while starting transaction: %w", err)
	}

	rootBucket := btx.Bucket([]byte(rootBucketName))
	wtx := &writeTx{
		btx:        btx,
		readOnly:   false,
		rootBucket: rootBucket,
	}

	var realWriteTx bolted.WriteTx = wtx

	for _, d := range b.writeTxDecorators {
		realWriteTx = d(realWriteTx)
	}

	return realWriteTx, nil

}

func (b *Bolted) beginRead() (*writeTx, error) {
	btx, err := b.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("while starting transaction: %w", err)
	}

	rootBucket := btx.Bucket([]byte(rootBucketName))
	wtx := &writeTx{
		btx:        btx,
		readOnly:   true,
		rootBucket: rootBucket,
	}
	return wtx, nil
}

func (b *Bolted) BeginRead() (bolted.ReadTx, error) {
	return b.beginRead()
}

func (b *Bolted) Observe(path dbpath.Matcher) (<-chan bolted.ObservedChanges, func()) {
	ev, cl := b.obs.observe(path)
	return ev, cl
}

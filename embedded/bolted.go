package embedded

import (
	"fmt"
	"os"

	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/dbpath"

	bolt "go.etcd.io/bbolt"
)

type Bolted struct {
	db              *bolt.DB
	changeListeners CompositeChangeListener
	obs             *observer
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode, options ...Option) (database.Bolted, error) {
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

func (b *Bolted) BeginWrite() (database.WriteTx, error) {
	btx, err := b.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("while starting transaction: %w", err)
	}

	wtx := &writeTx{
		btx:             btx,
		changeListeners: b.changeListeners,
		readOnly:        false,
	}

	err = b.changeListeners.Start(wtx)
	if err != nil {
		err2 := wtx.Rollback()
		if err2 != nil {
			return nil, err2
		}
		return nil, fmt.Errorf("change listener start: %w", err)
	}

	return wtx, nil

}

func (b *Bolted) beginRead() (*writeTx, error) {
	btx, err := b.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("while starting transaction: %w", err)
	}

	wtx := &writeTx{
		btx:             btx,
		changeListeners: b.changeListeners,
		readOnly:        true,
	}
	return wtx, nil
}

func (b *Bolted) BeginRead() (database.ReadTx, error) {
	return b.beginRead()
}

func (b *Bolted) Observe(path dbpath.Matcher) (<-chan database.ObservedChanges, func()) {
	ev, cl := b.obs.observe(path)
	return ev, cl
}

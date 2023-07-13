package local

import (
	"fmt"
	"os"

	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/dbt"

	"go.etcd.io/bbolt"
)

type LocalDB struct {
	db                *bbolt.DB
	obs               *observer
	writeTxDecorators []WriteTxDecorator
}

type Options struct {
	bbolt.Options
	WriteDecorators []WriteTxDecorator
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode, options Options) (*LocalDB, error) {
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

	b := &LocalDB{
		db:                db,
		obs:               obs,
		writeTxDecorators: []WriteTxDecorator{obs.writeTxDecorator},
	}

	b.writeTxDecorators = append(b.writeTxDecorators, options.WriteDecorators...)

	return b, nil

}

func (b *LocalDB) Close() error {
	err := b.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (b *LocalDB) Stats() (*bbolt.Stats, error) {
	st := b.db.Stats()
	return &st, nil
}

func (b *LocalDB) Write(fn func(tx dbt.WriteTx) error) error {
	return b.db.Update(func(btx *bbolt.Tx) (err error) {

		defer func() {

			v := recover()
			if v == nil {
				return
			}

			re, isError := v.(error)
			if isError {
				err = re
				return
			}

			err = fmt.Errorf("panic: %v", err)

		}()

		rootBucket := btx.Bucket([]byte(rootBucketName))
		wtx := &writeTx{
			btx:         btx,
			readOnly:    false,
			rootBucket:  rootBucket,
			fillPercent: bbolt.DefaultFillPercent,
		}

		var realWriteTx dbt.WriteTx = wtx

		wrappers := []dbt.WriteTx{realWriteTx}

		for _, d := range b.writeTxDecorators {
			realWriteTx = d(realWriteTx)
			wrappers = append(wrappers, realWriteTx)
		}

		err = fn(realWriteTx)
		if err == nil {
			for _, w := range wrappers {
				cl, isCommitListener := w.(CommitListener)
				if isCommitListener {
					cl.OnCommit()
				}
			}
		}
		return err
	})
}

func (b *LocalDB) Read(fn func(tx dbt.ReadTx) error) error {
	return b.db.View(func(btx *bbolt.Tx) (err error) {

		defer func() {

			v := recover()
			if v == nil {
				return
			}
			re, isError := v.(error)
			if isError {
				err = re
				return
			}

			err = fmt.Errorf("panic: %v", err)

		}()

		rootBucket := btx.Bucket([]byte(rootBucketName))
		tx := &writeTx{
			btx:         btx,
			readOnly:    true,
			rootBucket:  rootBucket,
			fillPercent: bbolt.DefaultFillPercent,
		}
		return fn(tx)
	})
}

func (b *LocalDB) Observe(path dbpath.Matcher) (<-chan dbt.ObservedChanges, func()) {
	ev, cl := b.obs.observe(path)
	return ev, cl
}

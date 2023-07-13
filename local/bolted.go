package local

import (
	"fmt"
	"os"

	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/dbt"

	"go.etcd.io/bbolt"
)

type LocalDB struct {
	path string
	db   *bbolt.DB
	obs  *observer
}

type Options struct {
	bbolt.Options
}

const rootBucketName = "root"

func Open(path string, mode os.FileMode, options Options) (*LocalDB, error) {
	db, err := bbolt.Open(path, mode, &options.Options)
	if err != nil {
		return nil, fmt.Errorf("while opening bolt db: %w", err)
	}

	var fileSize float64

	{
		tx, err := db.Begin(false)
		if err != nil {
			return nil, fmt.Errorf("while opening read tx: %w", err)
		}

		rootExists := tx.Bucket([]byte(rootBucketName)) != nil

		fileSize = float64(tx.Size())

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
				fileSize = float64(tx.Size())
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("while creating root bucket: %w", err)
			}
		}

	}

	obs := newObserver()

	b := &LocalDB{
		path: path,
		db:   db,
		obs:  obs,
	}

	initializeMetricsForDB(path, fileSize)

	return b, nil

}

func (b *LocalDB) Close() error {
	err := b.db.Close()
	if err != nil {
		return err
	}
	removeMetricsForDB(b.path)
	return nil
}

func (b *LocalDB) Stats() (*bbolt.Stats, error) {
	st := b.db.Stats()
	return &st, nil
}

func (b *LocalDB) Write(fn func(tx dbt.WriteTx) error) (err error) {
	txObserver := b.obs.newWTxObserver()

	defer func() {
		if err == nil {
			txObserver.broadcast()
		}
	}()

	return b.db.Update(func(btx *bbolt.Tx) (err error) {
		{
			cnt, err2 := numberOfWriteTransactionsVec.GetMetricWithLabelValues(b.path)
			if err2 == nil {
				cnt.Inc()
			}
		}
		defer func() {

			v := recover()
			if v == nil && err == nil {
				cnt, err2 := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(b.path)
				if err2 == nil {
					cnt.Inc()
				}
				g, err2 := dbFileSizeVec.GetMetricWithLabelValues(b.path)
				if err2 == nil {
					g.Set(float64(btx.Size()))
				}
				return
			}

			cnt, err2 := numberOfFailedTransactionsVec.GetMetricWithLabelValues(b.path)
			if err2 == nil {
				cnt.Inc()
			}

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
			observer:    txObserver,
		}

		return fn(wtx)
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

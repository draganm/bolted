package bolted

import (
	"fmt"

	"github.com/draganm/bolted/dbpath"
)

type SugaredWriteTx interface {
	GetRawWriteTX() WriteTx
	CreateMap(path dbpath.Path)
	Delete(path dbpath.Path)
	Put(path dbpath.Path, value []byte)
	SugaredReadTx
}

type SugaredReadTx interface {
	GetRawReadTX() ReadTx
	Get(path dbpath.Path) []byte
	Iterator(path dbpath.Path) SugaredIterator
	Exists(path dbpath.Path) bool
	IsMap(path dbpath.Path) bool
	Size(path dbpath.Path) uint64
	ID() uint64
}

type SugaredIterator interface {
	GetKey() string
	GetValue() []byte
	IsDone() bool
	Prev()
	Next()
	Seek(key string)
	First()
	Last()
}

type sugaredReadTx struct {
	tx ReadTx
}

func (rt sugaredReadTx) GetRawReadTX() ReadTx {
	return rt.tx
}

type sugaredWriteTx struct {
	tx WriteTx
	sugaredReadTx
}

func (rt sugaredWriteTx) GetRawWriteTX() WriteTx {
	return rt.tx
}

func (st sugaredWriteTx) CreateMap(path dbpath.Path) {
	err := st.tx.CreateMap(path)
	if err != nil {
		panic(err)
	}
}

func (st sugaredWriteTx) Delete(path dbpath.Path) {
	err := st.tx.Delete(path)
	if err != nil {
		panic(err)
	}
}

func (st sugaredWriteTx) Put(path dbpath.Path, value []byte) {
	err := st.tx.Put(path, value)
	if err != nil {
		panic(err)
	}
}

func (st sugaredReadTx) Get(path dbpath.Path) []byte {
	d, err := st.tx.Get(path)
	if err != nil {
		panic(err)
	}
	return d
}

func (st sugaredReadTx) ID() uint64 {
	id, err := st.tx.ID()
	if err != nil {
		panic(err)
	}
	return id
}

func (st sugaredReadTx) Iterator(path dbpath.Path) SugaredIterator {
	it, err := st.tx.Iterator(path)
	if err != nil {
		panic(err)
	}
	return &sugaredIterator{it: it}
}

func (st sugaredReadTx) IsMap(path dbpath.Path) bool {
	ism, err := st.tx.IsMap(path)
	if err != nil {
		panic(err)
	}
	return ism
}

func (st sugaredReadTx) Exists(path dbpath.Path) bool {
	ex, err := st.tx.Exists(path)
	if err != nil {
		panic(err)
	}
	return ex
}

func (st sugaredReadTx) Size(path dbpath.Path) uint64 {
	sz, err := st.tx.Size(path)
	if err != nil {
		panic(err)
	}
	return sz
}

type sugaredIterator struct {
	it Iterator
}

func (si sugaredIterator) GetKey() string {
	k, err := si.it.GetKey()
	if err != nil {
		panic(err)
	}
	return k
}

func (si sugaredIterator) GetValue() []byte {
	v, err := si.it.GetValue()
	if err != nil {
		panic(err)
	}
	return v
}

func (si sugaredIterator) IsDone() bool {
	d, err := si.it.IsDone()
	if err != nil {
		panic(err)
	}
	return d
}

func (si sugaredIterator) Prev() {
	err := si.it.Prev()
	if err != nil {
		panic(err)
	}
}

func (si sugaredIterator) Next() {
	err := si.it.Next()
	if err != nil {
		panic(err)
	}
}

func (si sugaredIterator) Seek(key string) {
	err := si.it.Seek(key)
	if err != nil {
		panic(err)
	}
}

func (si sugaredIterator) First() {
	err := si.it.First()
	if err != nil {
		panic(err)
	}
}

func (si sugaredIterator) Last() {
	err := si.it.Last()
	if err != nil {
		panic(err)
	}
}

func SugaredRead(db Database, f func(tx SugaredReadTx) error) (err error) {
	rtx, err := db.BeginRead()
	if err != nil {
		return fmt.Errorf("while starting write transaction: %w", err)
	}

	defer rtx.Finish()
	defer func() {
		re := recover()
		if re != nil {
			var isError bool
			err, isError = re.(error)
			if !isError {
				panic(re)
			}

		}
	}()

	return f(sugaredReadTx{tx: rtx})

}

func SugaredWrite(db Database, f func(tx SugaredWriteTx) error) (err error) {
	wtx, err := db.BeginWrite()
	if err != nil {
		return fmt.Errorf("while starting write transaction: %w", err)
	}

	tx := sugaredWriteTx{tx: wtx, sugaredReadTx: sugaredReadTx{tx: wtx}}

	defer func() {
		re := recover()
		if re != nil {
			var isError bool
			err, isError = re.(error)
			if !isError {
				panic(re)
			}

		}
	}()

	defer func() {
		if err != nil {
			_ = wtx.Rollback()
			// TODO: log rollback error?
		}
		fe := wtx.Finish()
		if fe != nil && err == nil {
			err = fe
		}
	}()

	return f(tx)

}

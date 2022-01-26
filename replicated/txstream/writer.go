package txstream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type Writer struct {
	log *bufio.Writer
	bolted.ReadTx
	nextIterator uint64
}

func NewWriter(tx bolted.ReadTx, log io.Writer) *Writer {
	return &Writer{
		log:    bufio.NewWriter(log),
		ReadTx: tx,
	}
}

func WriteAll(w *bufio.Writer, fns ...func(w *bufio.Writer) error) error {
	for _, f := range fns {
		err := f(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeVarUint64(v uint64) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		d := make([]byte, binary.MaxVarintLen64)
		l := binary.PutUvarint(d, v)
		_, err := w.Write(d[:l])
		return err
	}
}

func WriteByte(b byte) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		return w.WriteByte(b)
	}
}

func writeBool(v bool) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		if v {
			return w.WriteByte(1)
		}
		return w.WriteByte(0)
	}
}

func WritePath(path dbpath.Path) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		pathString := path.String()

		if len(pathString) > 65535-(1+binary.MaxVarintLen16) {
			return errors.New("dbpath is tool long")
		}

		data := make([]byte, binary.MaxVarintLen16)

		lenlen := binary.PutUvarint(data, uint64(len(pathString)))

		_, err := w.Write(data[:lenlen])
		if err != nil {
			return fmt.Errorf("while writing length: %w", err)
		}

		_, err = w.WriteString(pathString)

		if err != nil {
			return fmt.Errorf("while writing path: %w", err)
		}

		return nil

	}
}

func writeData(data []byte) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		ln := make([]byte, binary.MaxVarintLen64)

		lenlen := binary.PutUvarint(ln, uint64(len(data)))

		_, err := w.Write(ln[:lenlen])
		if err != nil {
			return fmt.Errorf("while writing length: %w", err)
		}

		_, err = w.Write(data)

		if err != nil {
			return fmt.Errorf("while writing data: %w", err)
		}

		return nil

	}
}

const CreateMap byte = 1

func (w *Writer) CreateMap(path dbpath.Path) error {
	return WriteAll(
		w.log,
		WriteByte(CreateMap),
		WritePath(path),
	)
}

const delete byte = 2

func (w *Writer) Delete(path dbpath.Path) error {

	ex, err := w.ReadTx.Exists(path)
	if err != nil {
		return err
	}

	if !ex {
		return bolted.ErrNotFound
	}

	// TODO: get!

	return WriteAll(
		w.log,
		WriteByte(delete),
		WritePath(path),
	)

}

const put byte = 3

func (w *Writer) Put(path dbpath.Path, value []byte) error {
	exists, err := w.Exists(path)
	if err != nil {
		return fmt.Errorf("while checking if value exists: %w", err)
	}

	if exists {
		isMap, err := w.IsMap(path)
		if err != nil {
			return fmt.Errorf("while checking if existing value is a map: %w", err)
		}

		if isMap {
			return bolted.ErrConflict
		}

		_, err = w.Get(path)
		if err != nil {
			return fmt.Errorf("while recording previous value: %w", err)
		}
	}

	return WriteAll(
		w.log,
		WriteByte(put),
		WritePath(path),
		writeData(value),
	)

}

func (w *Writer) Rollback() error {
	err := w.ReadTx.Finish()
	if err != nil {
		return err
	}

	return errors.New("rolled back")
}

const get byte = 6

func (w *Writer) Get(path dbpath.Path) ([]byte, error) {

	data, err := w.ReadTx.Get(path)
	if err != nil {
		return nil, fmt.Errorf("while getting data: %w", err)
	}

	err = WriteAll(
		w.log,
		WriteByte(get),
		WritePath(path),
		writeDataOrHash(data),
	)

	if err != nil {
		return nil, err
	}

	return data, nil

}

const exists byte = 4

func (w *Writer) Exists(path dbpath.Path) (bool, error) {

	ex, err := w.ReadTx.Exists(path)
	if err != nil {
		return false, fmt.Errorf("while checking if path exists: %w", err)
	}

	err = WriteAll(
		w.log,
		WriteByte(exists),
		WritePath(path),
		writeBool(ex),
	)

	if err != nil {
		return false, err
	}

	return ex, nil

}

const isMap byte = 5

func (w *Writer) IsMap(path dbpath.Path) (bool, error) {
	ism, err := w.ReadTx.IsMap(path)
	if err != nil {
		return false, fmt.Errorf("while checking if path is a map: %w", err)
	}

	err = WriteAll(
		w.log,
		WriteByte(isMap),
		WritePath(path),
		writeBool(ism),
	)

	if err != nil {
		return false, err
	}

	return ism, nil
}

const size byte = 7

func (w *Writer) Size(path dbpath.Path) (uint64, error) {
	s, err := w.ReadTx.Size(path)
	if err != nil {
		return 0, fmt.Errorf("while checking if path is a map: %w", err)
	}

	err = WriteAll(
		w.log,
		WriteByte(size),
		WritePath(path),
		writeVarUint64(s),
	)

	if err != nil {
		return 0, err
	}

	return s, nil
}

func (w *Writer) Finish() (err error) {
	defer func() {
		e := w.log.Flush()
		if err == nil {
			err = e
		}
	}()

	err = w.ReadTx.Finish()
	return err
}

const newIterator byte = 8

func (w *Writer) ID() (uint64, error) {
	id, err := w.ReadTx.ID()
	if err != nil {
		return 0, err
	}
	return id + 1, nil
}

func (w *Writer) Iterator(path dbpath.Path) (bolted.Iterator, error) {
	it, err := w.ReadTx.Iterator(path)
	if err != nil {
		return nil, fmt.Errorf("while creating iterator: %w", err)
	}

	idx := w.nextIterator
	w.nextIterator++

	err = WriteAll(
		w.log,
		WriteByte(newIterator),
		WritePath(path),
	)

	if err != nil {
		return nil, err
	}

	iw := &iteratorWriter{
		log: w.log,
		it:  it,
		idx: idx,
	}

	return iw, nil
}

type iteratorWriter struct {
	log *bufio.Writer
	it  bolted.Iterator
	idx uint64
}

const iteratorGetKey byte = 9

func (i *iteratorWriter) GetKey() (string, error) {
	k, err := i.it.GetKey()
	if err != nil {
		return "", err
	}

	err = WriteAll(
		i.log,
		WriteByte(iteratorGetKey),
		writeVarUint64(i.idx),
		writeData([]byte(k)),
	)

	if err != nil {
		return "", err
	}

	return k, nil
}

const iteratorGetValue byte = 10

func (i *iteratorWriter) GetValue() ([]byte, error) {
	v, err := i.it.GetValue()
	if err != nil {
		return nil, err
	}

	err = WriteAll(
		i.log,
		WriteByte(iteratorGetValue),
		writeVarUint64(i.idx),
		writeDataOrHash([]byte(v)),
	)

	if err != nil {
		return nil, err
	}

	return v, nil

}

const iteratorIsDone byte = 11

func (i *iteratorWriter) IsDone() (bool, error) {
	d, err := i.it.IsDone()
	if err != nil {
		return false, err
	}

	err = WriteAll(
		i.log,
		WriteByte(iteratorIsDone),
		writeVarUint64(i.idx),
		writeBool(d),
	)

	if err != nil {
		return false, err
	}

	return d, nil

}

const iteratorPrev byte = 12

func (i *iteratorWriter) Prev() error {
	err := i.it.Prev()
	if err != nil {
		return err
	}
	return WriteAll(
		i.log,
		WriteByte(iteratorPrev),
		writeVarUint64(i.idx),
	)
}

const iteratorNext byte = 13

func (i *iteratorWriter) Next() error {
	err := i.it.Next()
	if err != nil {
		return err
	}
	return WriteAll(
		i.log,
		WriteByte(iteratorNext),
		writeVarUint64(i.idx),
	)
}

const iteratorSeek byte = 14

func (i *iteratorWriter) Seek(key string) error {
	err := i.it.Seek(key)
	if err != nil {
		return err
	}
	return WriteAll(
		i.log,
		WriteByte(iteratorSeek),
		writeVarUint64(i.idx),
		writeData([]byte(key)),
	)
}

const iteratorFirst byte = 15

func (i *iteratorWriter) First() error {
	err := i.it.First()
	if err != nil {
		return err
	}

	return WriteAll(
		i.log,
		WriteByte(iteratorFirst),
		writeVarUint64(i.idx),
	)
}

const iteratorLast byte = 16

func (i *iteratorWriter) Last() error {
	err := i.it.Last()
	if err != nil {
		return err
	}

	return WriteAll(
		i.log,
		WriteByte(iteratorLast),
		writeVarUint64(i.idx),
	)
}

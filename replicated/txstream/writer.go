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
	log bufio.Writer
	bolted.ReadTx
}

func NewWriter(tx bolted.ReadTx, log io.Writer) *Writer {
	return &Writer{
		log:    *bufio.NewWriter(log),
		ReadTx: tx,
	}
}

const createMap byte = 1

func (w *Writer) writePath(path dbpath.Path) error {
	pathString := path.String()

	if len(pathString) > 65535-(1+binary.MaxVarintLen16) {
		return errors.New("dbpath is tool long")
	}

	data := make([]byte, binary.MaxVarintLen16)

	lenlen := binary.PutUvarint(data, uint64(len(pathString)))

	_, err := w.log.Write(data[:lenlen])
	if err != nil {
		return fmt.Errorf("while writing length: %w", err)
	}

	_, err = w.log.WriteString(pathString)

	if err != nil {
		return fmt.Errorf("while writing path: %w", err)
	}

	return nil
}

func (w *Writer) writeData(data []byte) error {

	ln := make([]byte, binary.MaxVarintLen64)

	lenlen := binary.PutUvarint(ln, uint64(len(data)))

	_, err := w.log.Write(ln[:lenlen])
	if err != nil {
		return fmt.Errorf("while writing length: %w", err)
	}

	_, err = w.log.Write(data)

	if err != nil {
		return fmt.Errorf("while writing data: %w", err)
	}

	return nil
}

func (w *Writer) CreateMap(path dbpath.Path) error {
	err := w.log.WriteByte(createMap)
	if err != nil {
		return fmt.Errorf("while writing createMap: %w", err)
	}

	return w.writePath(path)
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

	err = w.log.WriteByte(delete)
	if err != nil {
		return fmt.Errorf("while writing delete: %w", err)
	}

	return w.writePath(path)

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

	err = w.log.WriteByte(put)
	if err != nil {
		return fmt.Errorf("while writing put: %w", err)
	}

	err = w.writePath(path)

	if err != nil {
		return fmt.Errorf("while writing path: %w", err)
	}

	err = w.writeData(value)

	if err != nil {
		return fmt.Errorf("while writing value: %w", err)
	}

	return nil

}

func (w *Writer) Rollback() error {
	return errors.New("not yet implemented")
}

const get byte = 6

func (w *Writer) Get(path dbpath.Path) ([]byte, error) {

	data, err := w.ReadTx.Get(path)
	if err != nil {
		return nil, fmt.Errorf("while getting data: %w", err)
	}

	err = w.log.WriteByte(get)
	if err != nil {
		return nil, fmt.Errorf("while writing get: %w", err)
	}

	err = w.writePath(path)

	if err != nil {
		return nil, fmt.Errorf("while writing path: %w", err)
	}

	err = w.writeDataOrHash(data)

	if err != nil {
		return nil, fmt.Errorf("while writing value: %w", err)
	}

	return data, nil

}
func (w *Writer) Iterator(path dbpath.Path) (bolted.Iterator, error) {
	return nil, errors.New("not yet implemented")
}

const exists byte = 4

func (w *Writer) Exists(path dbpath.Path) (bool, error) {

	ex, err := w.ReadTx.Exists(path)
	if err != nil {
		return false, fmt.Errorf("while checking if path exists: %w", err)
	}

	err = w.log.WriteByte(exists)
	if err != nil {
		return false, fmt.Errorf("while writing put: %w", err)
	}

	err = w.writePath(path)

	if err != nil {
		return false, fmt.Errorf("while writing exists: %w", err)
	}

	existsData := byte(0)
	if ex {
		existsData = 1
	}

	err = w.log.WriteByte(existsData)
	if err != nil {
		return false, fmt.Errorf("while writing exists data: %w", err)
	}

	return ex, nil

}

const isMap byte = 5

func (w *Writer) IsMap(path dbpath.Path) (bool, error) {
	ism, err := w.ReadTx.IsMap(path)
	if err != nil {
		return false, fmt.Errorf("while checking if path is a map: %w", err)
	}

	err = w.log.WriteByte(isMap)
	if err != nil {
		return false, fmt.Errorf("while writing isMap: %w", err)
	}

	err = w.writePath(path)

	if err != nil {
		return false, fmt.Errorf("while writing path: %w", err)
	}

	isMapData := byte(0)
	if ism {
		isMapData = 1
	}

	err = w.log.WriteByte(isMapData)
	if err != nil {
		return false, fmt.Errorf("while writing is map data: %w", err)
	}

	return ism, nil
}

const size byte = 7

func (w *Writer) Size(path dbpath.Path) (uint64, error) {
	s, err := w.ReadTx.Size(path)
	if err != nil {
		return 0, fmt.Errorf("while checking if path is a map: %w", err)
	}

	err = w.log.WriteByte(size)
	if err != nil {
		return 0, fmt.Errorf("while writing size: %w", err)
	}

	err = w.writePath(path)

	if err != nil {
		return 0, fmt.Errorf("while writing path: %w", err)
	}

	data := make([]byte, binary.MaxVarintLen64)

	lenlen := binary.PutUvarint(data, s)

	_, err = w.log.Write(data[:lenlen])
	if err != nil {
		return 0, fmt.Errorf("while writing size: %w", err)
	}

	return s, nil
}

func (w *Writer) Finish() error {
	defer w.log.Flush()
	return w.ReadTx.Finish()
}

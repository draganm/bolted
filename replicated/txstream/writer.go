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

func (w *Writer) Put(path dbpath.Path, value []byte) error {
	return errors.New("not yet implemented")
}

func (w *Writer) Rollback() error {
	return errors.New("not yet implemented")
}

func (w *Writer) Get(path dbpath.Path) ([]byte, error) {
	return nil, errors.New("not yet implemented")
}
func (w *Writer) Iterator(path dbpath.Path) (bolted.Iterator, error) {
	return nil, errors.New("not yet implemented")
}
func (w *Writer) Exists(path dbpath.Path) (bool, error) {
	return false, errors.New("not yet implemented")
}
func (w *Writer) IsMap(path dbpath.Path) (bool, error) {
	return false, errors.New("not yet implemented")
}
func (w *Writer) Size(path dbpath.Path) (uint64, error) {
	return 0, errors.New("not yet implemented")
}

func (w *Writer) Finish() error {
	defer w.log.Flush()
	return w.ReadTx.Finish()
}

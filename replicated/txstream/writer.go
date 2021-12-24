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

// type WriteTx interface {
// 	CreateMap(path dbpath.Path) error
// 	Delete(path dbpath.Path) error
// 	Put(path dbpath.Path, value []byte) error
// 	Rollback() error
// 	ReadTx
// }

// type ReadTx interface {
// 	Get(path dbpath.Path) ([]byte, error)
// 	Iterator(path dbpath.Path) (Iterator, error)
// 	Exists(path dbpath.Path) (bool, error)
// 	IsMap(path dbpath.Path) (bool, error)
// 	Size(path dbpath.Path) (uint64, error)
// 	Finish() error
// }

const createMap byte = 1

func (w *Writer) CreateMap(path dbpath.Path) error {
	pathString := path.String()

	if len(pathString) > 65535-(1+binary.MaxVarintLen16) {
		return errors.New("dbpath is tool long")
	}

	err := w.log.WriteByte(createMap)
	if err != nil {
		return fmt.Errorf("while writing createMap: %w", err)
	}

	data := make([]byte, binary.MaxVarintLen16)

	lenlen := binary.PutUvarint(data, uint64(len(pathString)))

	_, err = w.log.Write(data[:lenlen])
	if err != nil {
		return fmt.Errorf("while writing length: %w", err)
	}

	_, err = w.log.WriteString(pathString)

	if err != nil {
		return fmt.Errorf("while writing path: %w", err)
	}

	return nil
}

func (w *Writer) Flush() error {
	return w.log.Flush()
}

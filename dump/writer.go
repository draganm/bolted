package dump

import (
	"encoding/binary"
	"io"
)

type DumpWriter struct {
	w io.Writer
}

func NewWriter(w io.Writer) DumpWriter {
	return DumpWriter{w: w}
}

type ItemType byte

const CreateMap ItemType = 0x1
const Put ItemType = 0x2

func (dw DumpWriter) CreateMap(name string) (int, error) {
	buf := make([]byte, binary.MaxVarintLen64+len(name)+1)
	buf[0] = byte(CreateMap)
	varSize := binary.PutUvarint(buf[1:], uint64(len(name)))
	copy(buf[1+varSize:], []byte(name))
	return dw.w.Write(buf[:1+varSize+len(name)])
}

func (dw DumpWriter) Put(name string, data []byte) (int, error) {
	buf := make([]byte, (2*binary.MaxVarintLen64)+len(name)+len(data)+1)
	buf[0] = byte(Put)
	cur := 1

	varSize := binary.PutUvarint(buf[cur:], uint64(len(name)))
	cur += varSize
	copy(buf[cur:], []byte(name))
	cur += len(name)

	varSize = binary.PutUvarint(buf[cur:], uint64(len(data)))
	cur += varSize
	copy(buf[cur:], data)
	cur += len(data)
	return dw.w.Write(buf[:cur])
}

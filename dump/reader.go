package dump

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) Reader {
	return Reader{
		br: bufio.NewReader(r),
	}
}

type Item struct {
	Type  ItemType
	Key   string
	Value []byte
}

func (r Reader) Next() (Item, error) {

	tb, err := r.br.ReadByte()
	if err != nil {
		return Item{}, err
	}

	switch ItemType(tb) {
	case CreateMap:
		key, err := r.readString()
		if err != nil {
			return Item{}, err
		}

		return Item{Type: CreateMap, Key: key}, nil
	case Put:
		key, err := r.readString()
		if err != nil {
			return Item{}, err
		}

		value, err := r.readData()
		if err != nil {
			return Item{}, err
		}

		return Item{Type: Put, Key: key, Value: value}, nil
	default:
		return Item{}, fmt.Errorf("unknown item type %d", tb)
	}
}

func (r Reader) readString() (string, error) {
	d, err := r.readData()
	if err != nil {
		return "", err
	}

	return string(d), nil
}

func (r Reader) readData() ([]byte, error) {

	s, err := binary.ReadUvarint(r.br)
	if err != nil {
		return nil, err
	}

	d := make([]byte, int(s))
	_, err = r.br.Read(d)
	if err != nil {
		return nil, err
	}

	return d, nil
}

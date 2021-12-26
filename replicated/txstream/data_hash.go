package txstream

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/draganm/bolted/replicated"
	"github.com/minio/highwayhash"
)

var key []byte

func init() {
	var err error
	key, err = hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000") // use your own key here
	if err != nil {
		panic(fmt.Errorf("while decoding highway key: %w", err))
	}
}

// func encodeDataOrHash(d []byte) error

func (w *Writer) writeDataOrHash(d []byte) error {
	if len(d) < 17 {
		err := w.log.WriteByte(byte(len(d)))
		if err != nil {
			return fmt.Errorf("while encoding data length: %w", err)
		}
		_, err = w.log.Write(d)
		if err != nil {
			return fmt.Errorf("while writing short data: %w", err)
		}
		return nil
	}

	hash, err := highwayhash.New(key)
	if err != nil {
		return fmt.Errorf("failed to create HighwayHash instance: %w", err)
	}

	h := hash.Sum(d)

	err = w.log.WriteByte(255)
	if err != nil {
		return fmt.Errorf("while writing hash header")
	}

	_, err = w.log.Write(h)
	if err != nil {
		return fmt.Errorf("while writing data HighwayHash: %w", err)
	}

	return nil

}

func verifyDataOrHash(r *bufio.Reader, data []byte) error {
	lenOrHashByte, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("while reading len or hash byte: %w", err)
	}

	if lenOrHashByte < 17 {
		d := make([]byte, lenOrHashByte)
		n, err := r.Read(d)
		if err != nil {
			return fmt.Errorf("while reading short data: %w", err)
		}

		if n != int(lenOrHashByte) {
			return fmt.Errorf("could not read complete short data, got %d, needed %d", n, lenOrHashByte)
		}

		if !bytes.Equal(data, d) {
			return replicated.ErrStale
		}
		return nil
	}

	if lenOrHashByte != 255 {
		return fmt.Errorf("unexpected hash mark %x", lenOrHashByte)
	}

	hd := make([]byte, 16)
	n, err := r.Read(hd)
	if err != nil {
		return fmt.Errorf("while reading hash: %w", err)
	}

	if n != 16 {
		return fmt.Errorf("could not read complete hash, expected 16 bytes, got %d", n)
	}

	hash, err := highwayhash.New(key)
	if err != nil {
		return fmt.Errorf("failed to create HighwayHash instance: %w", err)
	}

	h := hash.Sum(data)

	if !bytes.Equal(h, hd) {
		return replicated.ErrStale
	}

	return nil

}

package txstream

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"

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

func writeDataOrHash(d []byte) func(w *bufio.Writer) error {
	return func(w *bufio.Writer) error {
		if len(d) < 17 {
			err := w.WriteByte(byte(len(d)))
			if err != nil {
				return fmt.Errorf("while encoding data length: %w", err)
			}
			_, err = w.Write(d)
			if err != nil {
				return fmt.Errorf("while writing short data: %w", err)
			}
			return nil
		}

		hash, err := highwayhash.New128(key)
		if err != nil {
			return fmt.Errorf("failed to create HighwayHash instance: %w", err)
		}

		_, err = hash.Write(d)

		if err != nil {
			return fmt.Errorf("while writing to hash: %w", err)
		}

		h := hash.Sum(nil)

		err = w.WriteByte(255)
		if err != nil {
			return fmt.Errorf("while writing hash header")
		}

		_, err = w.Write(h)
		if err != nil {
			return fmt.Errorf("while writing data HighwayHash: %w", err)
		}

		return nil
	}
}

func verifyDataOrHash(r *bufio.Reader, data []byte) error {
	lenOrHashByte, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("while reading len or hash byte: %w", err)
	}

	if lenOrHashByte < 17 {
		d := make([]byte, lenOrHashByte)
		n, err := io.ReadFull(r, d)
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

	n, err := io.ReadFull(r, hd)
	if err != nil {
		return fmt.Errorf("while reading hash: %w", err)
	}

	if n != 16 {
		return fmt.Errorf("could not read complete hash, expected 16 bytes, got %d", n)
	}

	hash, err := highwayhash.New128(key)
	if err != nil {
		return fmt.Errorf("failed to create HighwayHash instance: %w", err)
	}

	_, err = hash.Write(data)
	if err != nil {
		return fmt.Errorf("while writing to hash: %w", err)
	}

	h := hash.Sum(nil)

	if !bytes.Equal(h, hd) {
		return fmt.Errorf("%s: expected hash %s, got %s", replicated.ErrStale, hex.EncodeToString(h), hex.EncodeToString(hd))
	}

	return nil

}

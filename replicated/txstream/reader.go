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

func Replay(r io.Reader, tx bolted.WriteTx) error {
	br := bufio.NewReader(r)

	t, err := br.ReadByte()

	if err == io.EOF {
		return nil
	}

	if err != nil {
		return fmt.Errorf("while reading type: %w", err)
	}

	switch t {
	case createMap:
		ln, err := binary.ReadUvarint(br)
		if err != nil {
			return fmt.Errorf("while reading path length")
		}

		sb := make([]byte, ln)

		n, err := br.Read(sb)
		if err != nil {
			return fmt.Errorf("while reading path: %w", err)
		}
		if n != int(ln) {
			return errors.New("could not read whole path")
		}

		pth, err := dbpath.Parse(string(sb))
		if err != nil {
			return fmt.Errorf("while parsing dbpath: %w", err)
		}

		err = tx.CreateMap(pth)
		if err != nil {
			return fmt.Errorf("while creating map: %w", err)
		}
	}
	return nil
}

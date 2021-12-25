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

func readPath(r *bufio.Reader) (dbpath.Path, error) {
	ln, err := binary.ReadUvarint(r)
	if err != nil {
		return dbpath.NilPath, fmt.Errorf("while reading path length")
	}

	sb := make([]byte, ln)

	n, err := r.Read(sb)
	if err != nil {
		return dbpath.NilPath, fmt.Errorf("while reading path: %w", err)
	}
	if n != int(ln) {
		return dbpath.NilPath, errors.New("could not read whole path")
	}

	pth, err := dbpath.Parse(string(sb))
	if err != nil {
		return dbpath.NilPath, fmt.Errorf("while parsing dbpath: %w", err)
	}

	return pth, nil
}

func Replay(r io.Reader, db bolted.Database) (err error) {
	tx, err := db.BeginWrite()
	if err != nil {
		return fmt.Errorf("while beginning tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	defer func() {
		tx.Finish()
	}()

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
		pth, err := readPath(br)
		if err != nil {
			return err
		}

		err = tx.CreateMap(pth)
		if err != nil {
			return fmt.Errorf("while creating map: %w", err)
		}
	case delete:
		pth, err := readPath(br)
		if err != nil {
			return err
		}

		err = tx.Delete(pth)
		if err != nil {
			return fmt.Errorf("while deleting: %w", err)
		}
	}

	return nil
}

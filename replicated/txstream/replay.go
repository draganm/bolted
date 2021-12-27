package txstream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/replicated"
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
		return dbpath.NilPath, fmt.Errorf("could not read the whole path: %d vs %d", n, ln)
	}

	pth, err := dbpath.Parse(string(sb))
	if err != nil {
		return dbpath.NilPath, fmt.Errorf("while parsing dbpath: %w", err)
	}

	return pth, nil
}

func readData(r *bufio.Reader) ([]byte, error) {
	ln, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("while reading data length")
	}

	data := make([]byte, ln)

	n, err := r.Read(data)
	if err != nil {
		return nil, fmt.Errorf("while reading data: %w", err)
	}

	if n != int(ln) {
		return nil, errors.New("could not read whole data")
	}

	return data, nil
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

	iterators := []bolted.Iterator{}

	for {

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

		case put:
			pth, err := readPath(br)
			if err != nil {
				return err
			}

			data, err := readData(br)
			if err != nil {
				return err
			}

			err = tx.Put(pth, data)
			if err != nil {
				return fmt.Errorf("while putting data: %w", err)
			}

		case exists:
			pth, err := readPath(br)
			if err != nil {
				return err
			}

			ex, err := br.ReadByte()
			if err != nil {
				return err
			}

			rex, err := tx.Exists(pth)
			if err != nil {
				return err
			}

			if rex != (ex != 0) {
				return replicated.ErrStale
			}

		case isMap:
			pth, err := readPath(br)
			if err != nil {
				return err
			}

			ism, err := br.ReadByte()
			if err != nil {
				return err
			}

			rism, err := tx.IsMap(pth)
			if err != nil {
				return err
			}

			if rism != (ism != 0) {
				return replicated.ErrStale
			}
		case get:
			pth, err := readPath(br)
			if err != nil {
				return err
			}

			d, err := tx.Get(pth)
			if err != nil {
				return fmt.Errorf("while getting local data: %w", err)
			}

			err = verifyDataOrHash(br, d)
			if err != nil {
				return err
			}
		case size:
			pth, err := readPath(br)
			if err != nil {
				return err
			}

			s, err := tx.Size(pth)
			if err != nil {
				return fmt.Errorf("while getting local path size: %w", err)
			}

			es, err := binary.ReadUvarint(br)
			if err != nil {
				return fmt.Errorf("while reading path size")
			}

			if s != es {
				return replicated.ErrStale
			}
		case newIterator:

			pth, err := readPath(br)
			if err != nil {
				return err
			}

			it, err := tx.Iterator(pth)
			if err != nil {
				return fmt.Errorf("while creating iterator: %w", err)
			}

			iterators = append(iterators, it)

		case iteratorIsDone:

			idx, err := binary.ReadUvarint(br)
			if err != nil {
				return fmt.Errorf("while reading iterator index: %w", err)
			}

			iidx := int(idx)

			if iidx >= len(iterators) {
				return fmt.Errorf("iterator index out of range")
			}

			isd, err := br.ReadByte()
			if err != nil {
				return fmt.Errorf("while getting isDone from log: %w", err)
			}

			it := iterators[iidx]
			id, err := it.IsDone()
			if err != nil {
				return fmt.Errorf("while getting is done status: %w", err)
			}

			if id != (isd != 0) {
				return replicated.ErrStale
			}

		default:
			return errors.New("unsupported operation")

		}
	}

	return nil
}

package txstream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

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

	n, err := io.ReadFull(r, sb)
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

	n, err := io.ReadFull(r, data)
	if err != nil {
		return nil, fmt.Errorf("while reading data: %w", err)
	}

	if n != int(ln) {
		return nil, errors.New("could not read whole data")
	}

	return data, nil
}

func Replay(r io.Reader, db bolted.Database) (txID uint64, err error) {
	tx, err := db.BeginWrite()
	if err != nil {
		return 0, fmt.Errorf("while beginning tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	defer func() {
		tx.Finish()
	}()

	txID, err = tx.ID()
	if err != nil {
		return 0, fmt.Errorf("while getting txID: %w", err)
	}

	br := bufio.NewReader(r)

	iterators := []bolted.Iterator{}

	getIterator := func() (bolted.Iterator, error) {
		idx, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, fmt.Errorf("while reading iterator index: %w", err)
		}

		iidx := int(idx)

		if iidx >= len(iterators) {
			return nil, fmt.Errorf("iterator index out of range")
		}

		return iterators[iidx], nil
	}
	for {

		t, err := br.ReadByte()

		if err == io.EOF {

			return txID, nil
		}

		if err != nil {
			return 0, fmt.Errorf("while reading type: %w", err)
		}

		switch t {
		case CreateMap:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			err = tx.CreateMap(pth)
			if err != nil {
				return 0, fmt.Errorf("while creating map: %w", err)
			}
		case delete:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			err = tx.Delete(pth)
			if err != nil {
				return 0, fmt.Errorf("while deleting: %w", err)
			}

		case put:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			data, err := readData(br)
			if err != nil {
				return 0, err
			}

			err = tx.Put(pth, data)
			if err != nil {
				return 0, fmt.Errorf("while putting data: %w", err)
			}

		case exists:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			ex, err := br.ReadByte()
			if err != nil {
				return 0, err
			}

			rex, err := tx.Exists(pth)
			if err != nil {
				return 0, err
			}

			if rex != (ex != 0) {
				return 0, fmt.Errorf("%w: checking exists of %s", replicated.ErrStale, pth.String())
			}

		case isMap:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			ism, err := br.ReadByte()
			if err != nil {
				return 0, err
			}

			rism, err := tx.IsMap(pth)
			if err != nil {
				return 0, err
			}

			if rism != (ism != 0) {
				return 0, fmt.Errorf("%w: checking is map of %s", replicated.ErrStale, pth.String())
			}
		case get:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			d, err := tx.Get(pth)
			if err != nil {
				return 0, fmt.Errorf("while getting local data: %w", err)
			}

			err = verifyDataOrHash(br, d)
			if err != nil {
				return 0, fmt.Errorf("%w: while performing get %s", err, pth.String())
			}
		case size:
			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			s, err := tx.Size(pth)
			if err != nil {
				return 0, fmt.Errorf("while getting local path size: %w", err)
			}

			es, err := binary.ReadUvarint(br)
			if err != nil {
				return 0, fmt.Errorf("while reading path size")
			}

			if s != es {
				return 0, fmt.Errorf("%w: checking size of %s", replicated.ErrStale, pth.String())
			}
		case newIterator:

			pth, err := readPath(br)
			if err != nil {
				return 0, err
			}

			it, err := tx.Iterator(pth)
			if err != nil {
				return 0, fmt.Errorf("while creating iterator: %w", err)
			}

			iterators = append(iterators, it)

		case iteratorIsDone:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			isd, err := br.ReadByte()
			if err != nil {
				return 0, fmt.Errorf("while getting isDone from log: %w", err)
			}

			id, err := it.IsDone()
			if err != nil {
				return 0, fmt.Errorf("while getting is done status: %w", err)
			}

			if id != (isd != 0) {
				// return 0, replicated.ErrStale
				return 0, fmt.Errorf("%w: checking iterator is done", replicated.ErrStale)
			}

		case iteratorNext:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			err = it.Next()
			if err != nil {
				return 0, fmt.Errorf("while performing next on the iterator: %w", err)
			}

		case iteratorGetKey:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			k, err := it.GetKey()
			if err != nil {
				return 0, fmt.Errorf("while getting iterator key: %w", err)
			}

			sk, err := readData(br)
			if err != nil {
				return 0, fmt.Errorf("while reading key from stream: %w", err)
			}

			if k != string(sk) {
				return 0, fmt.Errorf("%w: performing iterator get key", replicated.ErrStale)
			}
		case iteratorGetValue:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			v, err := it.GetValue()
			if err != nil {
				return 0, fmt.Errorf("while getting iterator value: %w", err)
			}

			err = verifyDataOrHash(br, v)
			if err != nil {
				return 0, fmt.Errorf("%w: while getting iteaotr value", err)
			}

		case iteratorFirst:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			err = it.First()
			if err != nil {
				return 0, err
			}

		case iteratorLast:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			err = it.Last()
			if err != nil {
				return 0, err
			}

		case iteratorSeek:

			it, err := getIterator()
			if err != nil {
				return 0, err
			}

			key, err := readData(br)
			if err != nil {
				return 0, fmt.Errorf("while reading key from stream: %w", err)
			}

			err = it.Seek(string(key))
			if err != nil {
				return 0, err
			}

		case SetFillPercent:
			fpint, err := binary.ReadUvarint(br)
			if err != nil {
				return 0, fmt.Errorf("while reading fill percent: %w", err)
			}
			fillPercent := math.Float64frombits(fpint)
			err = tx.SetFillPercent(fillPercent)
			if err != nil {
				return 0, err
			}

		default:
			return 0, errors.New("unsupported operation")

		}
	}

	// return nil
}

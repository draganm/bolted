package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/multierr"
)

type WAL struct {
	log         *os.File
	idx         *os.File
	mu          *sync.Mutex
	cond        *sync.Cond
	endPos      int64
	lastIndex   uint64
	indexOffset uint64
}

var ErrCorrupted = errors.New("WAL is corrupted")

func Open(name string) (w *WAL, err error) {
	log, err := os.OpenFile(fmt.Sprintf("%s.log", name), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0700)
	if err != nil {
		return nil, fmt.Errorf("while opening log: %w", err)
	}

	defer func() {
		if err != nil {
			log.Close()
		}
	}()

	logStat, err := log.Stat()
	if err != nil {
		return nil, fmt.Errorf("while getting stats of log: %w", err)
	}

	idx, err := os.OpenFile(fmt.Sprintf("%s.idx", name), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0700)
	if err != nil {
		return nil, fmt.Errorf("while opening index: %w", err)
	}

	defer func() {
		if err != nil {
			idx.Close()
		}
	}()

	idxStat, err := idx.Stat()
	if err != nil {
		return nil, fmt.Errorf("while getting stats of idx: %w", err)
	}

	idxSize := idxStat.Size()
	if idxSize%16 != 0 {
		return nil, fmt.Errorf("size of index file is invalid - not divisible by 16")
	}

	lastIdex := idxSize / 16
	mu := new(sync.Mutex)

	w = &WAL{
		log:         log,
		idx:         idx,
		mu:          mu,
		cond:        sync.NewCond(mu),
		endPos:      logStat.Size(),
		lastIndex:   uint64(lastIdex),
		indexOffset: 2,
	}

	logSize := logStat.Size()

	if idxSize == 0 && logSize == 0 {
		return w, nil
	}

	if idxSize == 0 && logSize > 0 {
		return nil, fmt.Errorf("%w: log has data while index is empty", ErrCorrupted)
	}

	if idxSize > 0 {
		lastIdxEntry := make([]byte, 16)
		idx.ReadAt(lastIdxEntry, idxSize-16)
		from := binary.BigEndian.Uint64(lastIdxEntry)
		len := binary.BigEndian.Uint64(lastIdxEntry[8:])

		if uint64(logSize) != from+len {
			return nil, fmt.Errorf("%w: log and index are out of sync", ErrCorrupted)
		}
	}

	return w, nil

}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return multierr.Append(w.log.Close(), w.idx.Close())
}

var ErrConflict = errors.New("index conflict")

func (w *WAL) Append(prevIndex uint64, r io.Reader) error {
	w.mu.Lock()

	// TODO: don't lock while writing
	defer w.mu.Unlock()

	lastLogIndex := w.lastIndex + w.indexOffset

	if lastLogIndex != prevIndex {
		return fmt.Errorf("%w: expected %d, got %d", ErrConflict, lastLogIndex, prevIndex)
	}

	startPos := w.endPos

	l, err := io.Copy(w.log, r)
	if err != nil {
		// TODO: truncate
		return ErrCorrupted
	}

	indexEntry := make([]byte, 16)
	binary.BigEndian.PutUint64(indexEntry, uint64(startPos))
	binary.BigEndian.PutUint64(indexEntry[8:], uint64(l))

	wb, err := w.idx.Write(indexEntry)
	if err != nil {
		// TODO: try fixing!
		return ErrCorrupted
	}

	if wb != 16 {
		// TODO: try fixing
		return ErrCorrupted
	}

	w.lastIndex++
	w.endPos += l

	w.cond.Broadcast()

	return nil

}

func (w *WAL) CopyOrWait(ctx context.Context, fromIndex uint64, wr io.Writer) error {

	endPosChan := make(chan int64, 1)
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		defer close(endPosChan)

		// if w.lastIndex+w.indexOffset > fromIndex {
		// 	endPosChan <- w.endPos
		// 	return
		// }

		// go func() {
		// 	<-ctx.Done()
		// 	close(endPosChan)
		// }()

		for w.lastIndex+w.indexOffset <= fromIndex {
			if ctx.Err() != nil {
				return
			}
			w.cond.Wait()
		}
		endPosChan <- w.endPos
	}()

	var endPos int64
	var ok bool
	select {
	case endPos, ok = <-endPosChan:
		if !ok {
			return ErrNoData
		}
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrNoData, ctx.Err())
	}

	return w.copyToWriter(fromIndex, uint64(endPos), wr)
}

var ErrNoData = errors.New("no data returned")

func (w *WAL) copyToWriter(fromIndex, endPos uint64, wr io.Writer) error {
	indexEntry := make([]byte, 16)

	_, err := w.idx.ReadAt(indexEntry, int64(fromIndex-w.indexOffset)*16)
	if err != nil {
		return fmt.Errorf("error reading index: %w", err)
	}

	fromPos := binary.BigEndian.Uint64(indexEntry)

	sr := io.NewSectionReader(w.log, int64(fromPos), int64(endPos-fromPos))

	_, err = io.Copy(wr, sr)
	if err != nil {
		return fmt.Errorf("while copying data: %w", err)
	}

	return nil

}

func (w *WAL) CopyNoWait(fromIndex uint64, wr io.Writer) error {
	w.mu.Lock()
	if w.lastIndex+w.indexOffset <= fromIndex {
		w.mu.Unlock()
		return ErrNoData
	}
	endPos := w.endPos
	w.mu.Unlock()

	return w.copyToWriter(fromIndex, uint64(endPos), wr)

}

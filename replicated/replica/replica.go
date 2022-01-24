package replica

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/bolted/replicated"
	"github.com/draganm/bolted/replicated/txstream"
	"github.com/draganm/bolted/util/flexbuffer"
)

type Replica interface {
	bolted.Database
}

type replica struct {
	bolted.Database
	cancel     func()
	primaryURL string
	ctx        context.Context
}

func Open(ctx context.Context, primaryURL, dbPath string) (Replica, error) {
	ctx, cancel := context.WithCancel(ctx)

	embedded, err := embedded.Open(dbPath, 0700)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("while opening local embedded db: %w", err)
	}

	r := &replica{
		primaryURL: primaryURL,
		Database:   embedded,
		cancel:     cancel,
		ctx:        ctx,
	}

	err = r.sync(0)
	if err != nil {
		return nil, fmt.Errorf("while performing initial sync: %w", err)
	}

	go func() {
		for ctx.Err() == nil {
			err := r.sync(30 * time.Second)
			if err != nil {
				fmt.Println("error polling:", err)
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	return r, nil
}

func (r *replica) sync(pollingPeriod time.Duration) error {
	var lastTXID uint64

	err := bolted.SugaredRead(r, func(tx bolted.SugaredReadTx) error {
		lastTXID = tx.ID()
		return nil
	})

	if err != nil {
		return fmt.Errorf("while getting txID: %w", err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/poll", r.primaryURL), nil)
	if err != nil {
		return fmt.Errorf("while creating GET request: %w", err)
	}

	q := req.URL.Query()
	q.Set("from", strconv.FormatUint(lastTXID, 10))
	q.Set("poll", pollingPeriod.String())
	req.URL.RawQuery = q.Encode()

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		if err != nil {
			return fmt.Errorf("while performing GET: %w", err)
		}
	}

	defer res.Body.Close()

	if res.StatusCode == 204 {
		return nil
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("unexpected status: %s", res.Status)
	}

	for ; ; lastTXID++ {
		br := bufio.NewReader(res.Body)
		txID, err := binary.ReadUvarint(br)
		if err == io.EOF {
			// all good, finish
			return nil
		}

		if err != nil {
			return fmt.Errorf("while reading txID: %w", err)
		}

		if lastTXID+1 != txID {
			return fmt.Errorf("received wrong transaction - %d instead of %d", txID, lastTXID+1)
		}

		txLen, err := binary.ReadUvarint(br)
		if err != nil {
			return fmt.Errorf("while reading next tx length: %w", err)
		}

		txReader := io.LimitReader(br, int64(txLen))

		_, err = txstream.Replay(txReader, r.Database)
		if err != nil {
			return fmt.Errorf("while replaying tx %d: %w", txID, err)
		}

	}

}

func (r *replica) Close() error {
	r.cancel()
	return r.Database.Close()
}

func (r *replica) BeginWrite() (bolted.WriteTx, error) {
	rtx, err := r.Database.BeginRead()
	if err != nil {
		return nil, fmt.Errorf("while creating new read tx: %w", err)
	}

	buf := flexbuffer.New(128 * 1024 * 1024)

	return &remoteWriteTx{
		primaryURL: r.primaryURL,
		Writer:     txstream.NewWriter(rtx, buf),
		buf:        buf,
		db:         r.Database,
	}, nil
}

type remoteWriteTx struct {
	primaryURL string
	*txstream.Writer
	buf      *flexbuffer.Flexbuffer
	commited bool
	db       bolted.Database
}

func writeUVariant(w io.Writer, val uint64) error {
	ln := make([]byte, binary.MaxVarintLen64)

	lenlen := binary.PutUvarint(ln, val)

	_, err := w.Write(ln[:lenlen])
	if err != nil {
		return fmt.Errorf("while writing val: %w", err)
	}

	return nil
}

func (r *remoteWriteTx) Finish() error {
	id, err := r.ReadTx.ID()
	if err != nil {
		return fmt.Errorf("while getting tx id: %w", err)
	}

	err = r.Writer.Finish()
	if err != nil {
		return err
	}

	postURL, err := url.Parse(r.primaryURL)
	if err != nil {
		return fmt.Errorf("while parsing primary URL: %w", err)
	}

	q := postURL.Query()
	q.Set("prev", fmt.Sprintf("%d", id))

	postURL.RawQuery = q.Encode()

	header := new(bytes.Buffer)

	txID := id + 1

	err = writeUVariant(header, txID)
	if err != nil {
		return fmt.Errorf("while writing tx id")
	}

	err = writeUVariant(header, uint64(r.buf.TotalSize))
	if err != nil {
		return fmt.Errorf("while writing tx total size")
	}

	req, err := http.NewRequest("POST", postURL.String(), io.MultiReader(header, r.buf))
	if err != nil {
		return fmt.Errorf("while creating POST request: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("while posting transaction to the primary: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode == 409 {
		return replicated.ErrStale
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("unexpected status: %s", res.Status)
	}

	observeChannel, cancelObserver := r.db.Observe(dbpath.NilPath.ToMatcher().AppendAnySubpathMatcher().AppendAnyElementMatcher())
	defer cancelObserver()

	var currentID uint64

	// this can be made much easier by adding a custom tx listener
	err = bolted.SugaredRead(r.db, func(tx bolted.SugaredReadTx) error {
		currentID = tx.ID()
		return nil
	})
	if err != nil {
		return fmt.Errorf("while reading current tx ID: %w", err)
	}

	if currentID >= txID {
		return nil
	}

	for range observeChannel {
		err = bolted.SugaredRead(r.db, func(tx bolted.SugaredReadTx) error {
			currentID = tx.ID()
			return nil
		})
		if err != nil {
			return fmt.Errorf("while reading current tx ID: %w", err)
		}

		if currentID >= txID {
			return nil
		}
	}
	return nil
}

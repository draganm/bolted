package replica

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/draganm/bolted"
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

	_, err := os.Stat(dbPath)
	switch {
	case os.IsNotExist(err):
		req, err := http.NewRequest("GET", primaryURL, nil)
		if err != nil {
			return nil, fmt.Errorf("while creating GET request: %w", err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("while downloading database: %w", err)
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			return nil, fmt.Errorf("unexpected server status when downloading db dump: %s", res.Status)
		}

		f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_WRONLY, 0700)
		if err != nil {
			return nil, fmt.Errorf("while creating db file: %w", err)
		}

		_, err = io.Copy(f, res.Body)
		if err != nil {
			return nil, fmt.Errorf("while downloading db file: %w", err)
		}

		err = f.Close()
		if err != nil {
			return nil, fmt.Errorf("while closing db file: %w", err)
		}

	case err != nil:
		return nil, fmt.Errorf("while getting stat of dbfile: %w", err)
	}

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

	go func() {
		for ctx.Err() == nil {
			fmt.Println("poll")
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
			fmt.Println("replicated")
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
		fmt.Println("replaying done")

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
	}, nil
}

type remoteWriteTx struct {
	primaryURL string
	*txstream.Writer
	buf      *flexbuffer.Flexbuffer
	commited bool
}

func (r *remoteWriteTx) Finish() error {
	err := r.Writer.Finish()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", r.primaryURL, r.buf)
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

	// TODO: wait for tx to be replicated
	time.Sleep(500 * time.Millisecond)
	return nil
}

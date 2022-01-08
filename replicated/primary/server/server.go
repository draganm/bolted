package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/bolted/replicated"
	"github.com/draganm/bolted/replicated/txstream"
	"github.com/gorilla/mux"
)

type Server struct {
	*mux.Router
	db embedded.DumpableDatabase
	// txDir string
	transactions map[uint64][]byte
	mu           *sync.Mutex
	cnd          *sync.Cond
	lastTxID     uint64
}

func New(db embedded.DumpableDatabase) (*Server, error) {
	r := mux.NewRouter()

	var lastTxID uint64

	err := bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
		lastTxID = tx.ID()
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("while getting txID: %w", err)
	}

	mu := new(sync.Mutex)
	s := &Server{
		Router:       r,
		db:           db,
		mu:           mu,
		cnd:          sync.NewCond(mu),
		transactions: make(map[uint64][]byte),
		lastTxID:     lastTxID,
	}
	r.Methods("GET").Path("/db").HandlerFunc(s.dump)
	r.Methods("GET").Path("/db/poll").HandlerFunc(s.pollForUpdates)
	r.Methods("POST").Path("/db").HandlerFunc(s.commit)

	return s, nil
}

func (p *Server) pollForUpdates(rw http.ResponseWriter, r *http.Request) {

	from, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	if err != nil {
		http.Error(rw, fmt.Errorf("while parsing from query parameter: %w", err).Error(), 400)
		return
	}

	poll, err := time.ParseDuration(r.URL.Query().Get("poll"))
	if err != nil {
		http.Error(rw, fmt.Errorf("while parsing poll query parameter: %w", err).Error(), 400)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), poll)
	defer cancel()

	p.mu.Lock()
	lastTxID := p.lastTxID
	p.mu.Unlock()

	if from >= lastTxID {
		txAvailable := make(chan struct{})
		go func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			for {
				if from < p.lastTxID {
					break
				}
				p.cnd.Wait()
			}
			close(txAvailable)
		}()

		fmt.Println("---- waiting for tx")

		select {
		case <-ctx.Done():
			// nothing to do,
			rw.WriteHeader(204)
			return
		case <-txAvailable:
			break
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Println("---- returning from", from, "to", p.lastTxID)

	if from < p.lastTxID {
		bw := bufio.NewWriter(rw)
		for txID := from + 1; txID <= p.lastTxID; txID++ {
			uvbuf := make([]byte, binary.MaxVarintLen64)

			l := binary.PutUvarint(uvbuf, txID)
			_, err = bw.Write(uvbuf[:l])
			if err != nil {
				// log?
				return
			}

			d := p.transactions[txID]

			l = binary.PutUvarint(uvbuf, uint64(len(d)))
			_, err = bw.Write(uvbuf[:l])
			if err != nil {
				// log?
				return
			}

			_, err = bw.Write(d)
			if err != nil {
				// log?
				return
			}
		}
		bw.Flush()
	}

}

func (p *Server) dump(rw http.ResponseWriter, r *http.Request) {
	p.db.Dump(rw)
}

func (p *Server) commit(rw http.ResponseWriter, r *http.Request) {

	p.mu.Lock()

	defer p.mu.Unlock()

	txb := new(bytes.Buffer)
	tr := io.TeeReader(r.Body, txb)

	txID, err := txstream.Replay(tr, p.db)
	if replicated.IsStale(err) {
		// rw.WriteHeader(409)
		http.Error(rw, err.Error(), http.StatusConflict)
		return
	}

	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	p.transactions[txID] = txb.Bytes()
	p.lastTxID = txID
	p.cnd.Broadcast()

	rw.Write([]byte(fmt.Sprintf("%d", txID)))

}

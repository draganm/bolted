package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/draganm/bolted/replicated/primary/server/wal"
	"github.com/gorilla/mux"
)

type Server struct {
	*mux.Router
	wal *wal.WAL
}

func New(logName string) (*Server, error) {
	r := mux.NewRouter()

	w, err := wal.Open(logName)
	if err != nil {
		return nil, fmt.Errorf("while opening WAL: %w", err)
	}

	s := &Server{
		Router: r,
		wal:    w,
	}
	// r.Methods("GET").Path("/db").HandlerFunc(s.dump)
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
	if poll > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), poll)
		err = p.wal.CopyOrWait(ctx, from, rw)
		cancel()
	} else {
		err = p.wal.CopyNoWait(from, rw)
	}

	if errors.Is(err, wal.ErrNoData) {
		http.Error(rw, err.Error(), 204)
		return
	}

	if err != nil {
		http.Error(rw, err.Error(), 500)
	}

}

func (p *Server) commit(rw http.ResponseWriter, r *http.Request) {
	prev, err := strconv.ParseUint(r.URL.Query().Get("prev"), 10, 64)
	if err != nil {
		http.Error(rw, fmt.Errorf("while parsing prev query parameter: %w", err).Error(), 400)
		return
	}

	err = p.wal.Append(prev, r.Body)

	switch {
	case errors.Is(err, wal.ErrConflict):
		http.Error(rw, err.Error(), 409)
	case err != nil:
		http.Error(rw, err.Error(), 500)
	}

}

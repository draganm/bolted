package watcher

import (
	"context"
	"strings"
	"sync"

	"github.com/draganm/bolted"
)

type Watcher struct {
	mu        *sync.Mutex
	observers observerList
	db        *bolted.Bolted

	curentTx map[string]bool
}

type observer struct {
	path    string
	cond    *sync.Cond
	mu      *sync.Mutex
	blocked bool
}

func (o *observer) wait() {
	o.mu.Lock()

	defer o.mu.Unlock()

	for o.blocked {
		o.cond.Wait()
	}
	o.blocked = true

	return
}

func (o *observer) wakeUp() {
	o.mu.Lock()
	o.blocked = false
	o.cond.Signal()
	o.mu.Unlock()
}

func (o *observer) isInterestedIn(path string) bool {
	return strings.HasPrefix(path, o.path)
}

func newObserver(path string) *observer {
	mu := new(sync.Mutex)
	return &observer{
		path:    path,
		mu:      mu,
		cond:    sync.NewCond(mu),
		blocked: true,
	}
}

type observerList []*observer

func New() *Watcher {
	return &Watcher{
		mu: new(sync.Mutex),
	}
}

func (w *Watcher) WatchForChanges(ctx context.Context, path string, cb func(c bolted.ReadTx) error) error {
	w.mu.Lock()
	observer := newObserver(path)
	w.observers = append(w.observers, observer)
	w.mu.Unlock()

	defer func() {
		// remove observer when done
		w.mu.Lock()
		newObservers := make(observerList, len(w.observers)-1)
		i := 0
		for _, o := range w.observers {
			if o != observer {
				newObservers[i] = o
				i++
			}
		}

		w.observers = newObservers
		w.mu.Unlock()
	}()

	// if context has Done channel, wait for it to close and wake up the observer
	dc := ctx.Done()
	if dc != nil {
		go func() {

			var ok = true
			for ok {
				_, ok = <-dc
			}

			observer.wakeUp()
		}()
	}

	for {
		err := w.db.Read(cb)
		if err != nil {
			return err
		}
		observer.wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return nil
}

func (w *Watcher) Added(b *bolted.Bolted) error {
	w.mu.Lock()
	w.db = b
	w.mu.Unlock()
	return nil
}

func (w *Watcher) Start(c bolted.WriteTx) error {
	w.mu.Lock()
	w.curentTx = map[string]bool{}
	w.mu.Unlock()
	return nil
}

func (w *Watcher) checkForInterested(path string) {
	w.mu.Lock()
	for _, o := range w.observers {
		if o.isInterestedIn(path) {
			w.curentTx[path] = true
		}
	}
	w.mu.Unlock()
}

func (w *Watcher) Delete(tx bolted.WriteTx, path string) error {
	w.checkForInterested(path)
	return nil
}

func (w *Watcher) CreateMap(tx bolted.WriteTx, path string) error {
	w.checkForInterested(path)
	return nil
}

func (w *Watcher) Put(tx bolted.WriteTx, path string, newValue []byte) error {
	w.checkForInterested(path)
	return nil
}

func (w *Watcher) BeforeCommit(tx bolted.WriteTx) error {
	return nil
}

func (w *Watcher) AfterTransaction(err error) error {
	w.mu.Lock()
	interested := []*observer{}

	if err == nil {
		// TODO: quadratic complexity, this can be done better!
		for _, o := range w.observers {
			for p := range w.curentTx {
				if o.isInterestedIn(p) {
					interested = append(interested, o)
					break
				}
			}
		}

		for _, o := range interested {
			o.wakeUp()
		}
	}
	w.mu.Unlock()
	return nil
}

package bolted

import (
	"strings"
	"sync"
)

type observer struct {
	mu              *sync.Mutex
	observers       map[int]*receiver
	nextObserverKey int
}

type receiver struct {
	path       string
	eventsChan chan ObservedEvent
	event      ObservedEvent
}

func (r *receiver) reset() {
	r.event = make(ObservedEvent)
}

func (r *receiver) handleEvent(path string, t ChangeType) {

	switch t {
	case Deleted:
		// delete from event all children
		for k := range r.event {
			if strings.HasPrefix(k, path) {
				delete(r.event, k)
			}
		}

		if strings.HasPrefix(r.path, path) {
			r.event[r.path] = Deleted
		}

		if strings.HasPrefix(r.path, path) {
			r.event[r.path] = Deleted
		}

		if strings.HasPrefix(path, r.path) {
			r.event[path] = Deleted
		}

	case ValueSet:
		if strings.HasPrefix(path, r.path) {
			r.event[path] = ValueSet
		}
	case MapCreated:
		if strings.HasPrefix(path, r.path) {
			r.event[path] = MapCreated
		}
	}
}

func (r *receiver) broadcast() {
	// TODO add unbound channel?
	select {
	case r.eventsChan <- r.event:
		// all good, just don't block
	default:
		// channel is full
	}
	r.event = nil
}

func newReceiver(path string) *receiver {
	return &receiver{
		path:       path,
		eventsChan: make(chan ObservedEvent, 1),
	}
}

func newObserver() *observer {
	return &observer{
		mu:        new(sync.Mutex),
		observers: make(map[int]*receiver),
	}
}

type ChangeType int

const (
	Unknown ChangeType = iota
	MapCreated
	ValueSet
	Deleted
)

type ObservedEvent map[string]ChangeType

func (w *observer) observePath(path string) (chan ObservedEvent, func()) {
	w.mu.Lock()
	observer := newReceiver(path)
	observerKey := w.nextObserverKey
	w.observers[observerKey] = observer
	w.nextObserverKey++
	w.mu.Unlock()

	closed := false

	return observer.eventsChan, func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if closed {
			return
		}

		delete(w.observers, observerKey)
		close(observer.eventsChan)
		closed = true
	}

}

func (w *observer) Opened(b *Bolted) error {
	return nil
}

func (w *observer) Start(c WriteTx) error {
	w.mu.Lock()
	for _, o := range w.observers {
		o.reset()
	}
	w.mu.Unlock()
	return nil
}

func (w *observer) updateObservers(path string, t ChangeType) {
	w.mu.Lock()
	for _, o := range w.observers {
		o.handleEvent(path, t)
	}
	w.mu.Unlock()
}

func (w *observer) Delete(tx WriteTx, path string) error {
	w.updateObservers(path, Deleted)
	return nil
}

func (w *observer) CreateMap(tx WriteTx, path string) error {
	w.updateObservers(path, MapCreated)
	return nil
}

func (w *observer) Put(tx WriteTx, path string, newValue []byte) error {
	w.updateObservers(path, ValueSet)
	return nil
}

func (w *observer) BeforeCommit(tx WriteTx) error {
	return nil
}

func (w *observer) AfterTransaction(err error) error {
	w.mu.Lock()

	if err == nil {
		for _, o := range w.observers {
			o.broadcast()
		}
	}
	w.mu.Unlock()
	return nil
}

// Closed TODO: add closing of the database semantics
func (w *observer) Closed() error {
	return nil
}

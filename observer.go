package bolted

import (
	"sync"

	"github.com/draganm/bolted/dbpath"
)

type observer struct {
	mu              *sync.Mutex
	observers       map[int]*receiver
	nextObserverKey int
}

type receiver struct {
	m          dbpath.Matcher
	eventsChan chan ObservedChanges
	event      ObservedChanges
}

func (r *receiver) reset() {
	r.event = nil
}

func (r *receiver) handleEvent(path dbpath.Path, t ChangeType) {

	if t == ChangeTypeDeleted || r.m.Matches(path) {
		r.event = r.event.update(path, t)
	}

}

func (r *receiver) broadcast() {
	if len(r.event) == 0 {
		return
	}
	// TODO add unbound channel?
	select {
	case r.eventsChan <- r.event:
		// all good, just don't block
	default:
		// channel is full
	}
	r.event = nil
}

func newReceiver(m dbpath.Matcher) *receiver {
	ch := make(chan ObservedChanges, 1)
	ch <- ObservedChanges{}
	return &receiver{
		m:          m,
		eventsChan: ch,
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
	ChangeTypeMapCreated
	ChangeTypeValueSet
	ChangeTypeDeleted
)

// type ObservedChanges map[string]ChangeType
type ObservedChange struct {
	Path dbpath.Path
	Type ChangeType
}

type ObservedChanges []ObservedChange

func (o ObservedChanges) update(path dbpath.Path, t ChangeType) ObservedChanges {
	switch t {
	case ChangeTypeValueSet, ChangeTypeMapCreated:
		for i, oc := range o {
			if oc.Path.Equal(path) {
				o[i].Type = t
				return o
			}
		}
		return append(o, ObservedChange{Path: path, Type: t})
	case ChangeTypeDeleted:
		m := path.ToMatcher().AppendAnySubpathMatcher()
		oc := ObservedChanges{}
		for _, c := range o {
			if !m.Matches(c.Path) {
				oc = append(oc, c)
			}
		}

		oc = append(oc, ObservedChange{Path: path, Type: t})
		return oc
	default:
		return o
	}
}

func (w *observer) observe(m dbpath.Matcher) (chan ObservedChanges, func()) {
	w.mu.Lock()
	observer := newReceiver(m)
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

func (w *observer) updateObservers(path dbpath.Path, t ChangeType) {
	w.mu.Lock()
	for _, o := range w.observers {
		o.handleEvent(path, t)
	}
	w.mu.Unlock()
}

func (w *observer) Delete(tx WriteTx, path dbpath.Path) error {
	w.updateObservers(path, ChangeTypeDeleted)
	return nil
}

func (w *observer) CreateMap(tx WriteTx, path dbpath.Path) error {
	w.updateObservers(path, ChangeTypeMapCreated)
	return nil
}

func (w *observer) Put(tx WriteTx, path dbpath.Path, newValue []byte) error {
	w.updateObservers(path, ChangeTypeValueSet)
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

package embedded

import (
	"sync"

	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/dbpath"
)

type observer struct {
	mu              *sync.Mutex
	observers       map[int]*receiver
	nextObserverKey int
}

type receiver struct {
	m dbpath.Matcher

	eventsChan chan<- database.ObservedChanges

	event database.ObservedChanges
}

func (r *receiver) reset() {
	r.event = nil
}

func (r *receiver) handleEvent(path dbpath.Path, t database.ChangeType) {

	if t == database.ChangeTypeDeleted || r.m.Matches(path) {
		r.event = r.event.Update(path, t)
	}

}

func (r *receiver) broadcast() {
	if len(r.event) == 0 {
		return
	}
	r.eventsChan <- r.event
	r.event = nil
}

func newReceiver(m dbpath.Matcher) (*receiver, <-chan database.ObservedChanges) {
	ch := make(chan database.ObservedChanges, 1)
	ch <- database.ObservedChanges{}

	incoming := make(chan database.ObservedChanges, 1)

	go func() {
		buffer := []database.ObservedChanges{}

		for {
			if len(buffer) == 0 {
				ev, ok := <-incoming
				if !ok {
					// reading cancelled
					close(ch)
					return
				}
				select {
				case ch <- ev:
					// all good
				default:
					// ok, have to wait
					buffer = append(buffer, ev)
				}
				continue
			}
			select {
			case ch <- buffer[0]:
				buffer = buffer[1:]
			case ev, ok := <-incoming:
				if !ok {
					// reading cancelled
					close(ch)
					return

				}
				buffer = append(buffer, ev)
			}
		}

	}()

	return &receiver{
		m:          m,
		eventsChan: ch,
	}, ch
}

func (r *receiver) close() {
	close(r.eventsChan)
}

func newObserver() *observer {
	return &observer{
		mu:        new(sync.Mutex),
		observers: make(map[int]*receiver),
	}
}

func (w *observer) observe(m dbpath.Matcher) (<-chan database.ObservedChanges, func()) {
	w.mu.Lock()
	observer, changesChan := newReceiver(m)
	observerKey := w.nextObserverKey
	w.observers[observerKey] = observer
	w.nextObserverKey++
	w.mu.Unlock()

	closed := false

	return changesChan, func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if closed {
			return
		}

		delete(w.observers, observerKey)
		observer.close()
		closed = true
	}

}

func (w *observer) Opened(b *Bolted) error {
	return nil
}

func (w *observer) Start(c database.WriteTx) error {
	w.mu.Lock()
	for _, o := range w.observers {
		o.reset()
	}
	w.mu.Unlock()
	return nil
}

func (w *observer) updateObservers(path dbpath.Path, t database.ChangeType) {
	w.mu.Lock()
	for _, o := range w.observers {
		o.handleEvent(path, t)
	}
	w.mu.Unlock()
}

func (w *observer) Delete(tx database.WriteTx, path dbpath.Path) error {
	w.updateObservers(path, database.ChangeTypeDeleted)
	return nil
}

func (w *observer) CreateMap(tx database.WriteTx, path dbpath.Path) error {
	w.updateObservers(path, database.ChangeTypeMapCreated)
	return nil
}

func (w *observer) Put(tx database.WriteTx, path dbpath.Path, newValue []byte) error {
	w.updateObservers(path, database.ChangeTypeValueSet)
	return nil
}

func (w *observer) BeforeCommit(tx database.WriteTx) error {
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

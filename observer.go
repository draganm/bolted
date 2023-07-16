package bolted

import (
	"sync"

	"github.com/draganm/bolted/dbpath"
)

type observer struct {
	mu              *sync.RWMutex
	receivers       map[int]*receiver
	nextReceiverKey int
}

func (o *observer) broadcastChanges(changes ObservedChanges) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, r := range o.receivers {
		r.notify(changes)
	}
}

type receiver struct {
	m dbpath.Matcher

	eventsChan chan<- ObservedChanges
	incoming   chan<- ObservedChanges
}

func (r *receiver) notify(changes ObservedChanges) {

	matchingChanges := ObservedChanges{}

	for _, ch := range changes {
		if ch.Type == ChangeTypeDeleted || r.m.Matches(ch.Path) {
			matchingChanges = matchingChanges.Update(ch.Path, ch.Type)
		}
	}

	if len(matchingChanges) == 0 {
		return
	}

	r.incoming <- matchingChanges

}

func newReceiver(m dbpath.Matcher) (*receiver, <-chan ObservedChanges) {
	ch := make(chan ObservedChanges, 1)
	ch <- ObservedChanges{}

	incoming := make(chan ObservedChanges, 1)

	go func() {
		buffer := []ObservedChanges{}

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
		incoming:   incoming,
	}, ch
}

func (r *receiver) close() {
	close(r.incoming)
}

func newObserver() *observer {
	return &observer{
		mu:        new(sync.RWMutex),
		receivers: make(map[int]*receiver),
	}
}

func (w *observer) observe(m dbpath.Matcher) (<-chan ObservedChanges, func()) {
	w.mu.Lock()
	receiver, changesChan := newReceiver(m)
	receiverKey := w.nextReceiverKey
	w.receivers[receiverKey] = receiver
	w.nextReceiverKey++
	w.mu.Unlock()

	closed := false

	return changesChan, func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if closed {
			return
		}

		delete(w.receivers, receiverKey)
		receiver.close()
		closed = true
	}

}

type txObserver struct {
	o       *observer
	changes []ObservedChange
}

func (o *observer) newWTxObserver() *txObserver {
	return &txObserver{
		o: o,
	}
}

func (to *txObserver) delete(path dbpath.Path) {

	to.changes = append(to.changes, ObservedChange{
		Path: path,
		Type: ChangeTypeDeleted,
	})
}

func (to *txObserver) createMap(path dbpath.Path) {

	to.changes = append(to.changes, ObservedChange{
		Path: path,
		Type: ChangeTypeMapCreated,
	})
}

func (to *txObserver) put(path dbpath.Path) {
	to.changes = append(to.changes, ObservedChange{
		Path: path,
		Type: ChangeTypeValueSet,
	})

}

func (to *txObserver) broadcast() {
	to.o.broadcastChanges(to.changes)
}

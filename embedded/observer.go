package embedded

import (
	"sync"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type observer struct {
	mu              *sync.RWMutex
	receivers       map[int]*receiver
	nextReceiverKey int
}

func (o *observer) broadcastChanges(changes bolted.ObservedChanges) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, r := range o.receivers {
		r.notify(changes)
	}
}

type receiver struct {
	m dbpath.Matcher

	eventsChan chan<- bolted.ObservedChanges
	incoming   chan<- bolted.ObservedChanges
}

func (r *receiver) notify(changes bolted.ObservedChanges) {

	matchingChanges := bolted.ObservedChanges{}

	for _, ch := range changes {
		if ch.Type == bolted.ChangeTypeDeleted || r.m.Matches(ch.Path) {
			matchingChanges = matchingChanges.Update(ch.Path, ch.Type)
		}
	}

	if len(matchingChanges) == 0 {
		return
	}

	r.incoming <- matchingChanges

}

func newReceiver(m dbpath.Matcher) (*receiver, <-chan bolted.ObservedChanges) {
	ch := make(chan bolted.ObservedChanges, 1)
	ch <- bolted.ObservedChanges{}

	incoming := make(chan bolted.ObservedChanges, 1)

	go func() {
		buffer := []bolted.ObservedChanges{}

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

func (w *observer) observe(m dbpath.Matcher) (<-chan bolted.ObservedChanges, func()) {
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
	o *observer
	bolted.WriteTx
	changes []bolted.ObservedChange
}

func (o *observer) writeTxDecorator(tx bolted.WriteTx) bolted.WriteTx {
	return &txObserver{
		o:       o,
		WriteTx: tx,
	}
}

func (to *txObserver) Delete(path dbpath.Path) error {
	err := to.WriteTx.Delete(path)
	if err != nil {
		return err
	}

	to.changes = append(to.changes, bolted.ObservedChange{
		Path: path,
		Type: bolted.ChangeTypeDeleted,
	})
	return nil
}

func (to *txObserver) CreateMap(path dbpath.Path) error {
	err := to.WriteTx.CreateMap(path)
	if err != nil {
		return err
	}

	to.changes = append(to.changes, bolted.ObservedChange{
		Path: path,
		Type: bolted.ChangeTypeMapCreated,
	})
	return nil
}

func (to *txObserver) Put(path dbpath.Path, data []byte) error {
	err := to.WriteTx.Put(path, data)
	if err != nil {
		return err
	}

	to.changes = append(to.changes, bolted.ObservedChange{
		Path: path,
		Type: bolted.ChangeTypeValueSet,
	})

	return nil
}

func (to *txObserver) Finish() error {
	err := to.WriteTx.Finish()
	if err != nil {
		return err
	}

	to.o.broadcastChanges(to.changes)

	return nil
}

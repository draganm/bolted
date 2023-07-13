package local

import (
	"sync"

	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/dbt"
)

type observer struct {
	mu              *sync.RWMutex
	receivers       map[int]*receiver
	nextReceiverKey int
}

func (o *observer) broadcastChanges(changes dbt.ObservedChanges) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, r := range o.receivers {
		r.notify(changes)
	}
}

type receiver struct {
	m dbpath.Matcher

	eventsChan chan<- dbt.ObservedChanges
	incoming   chan<- dbt.ObservedChanges
}

func (r *receiver) notify(changes dbt.ObservedChanges) {

	matchingChanges := dbt.ObservedChanges{}

	for _, ch := range changes {
		if ch.Type == dbt.ChangeTypeDeleted || r.m.Matches(ch.Path) {
			matchingChanges = matchingChanges.Update(ch.Path, ch.Type)
		}
	}

	if len(matchingChanges) == 0 {
		return
	}

	r.incoming <- matchingChanges

}

func newReceiver(m dbpath.Matcher) (*receiver, <-chan dbt.ObservedChanges) {
	ch := make(chan dbt.ObservedChanges, 1)
	ch <- dbt.ObservedChanges{}

	incoming := make(chan dbt.ObservedChanges, 1)

	go func() {
		buffer := []dbt.ObservedChanges{}

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

func (w *observer) observe(m dbpath.Matcher) (<-chan dbt.ObservedChanges, func()) {
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
	dbt.WriteTx
	changes []dbt.ObservedChange
}

func (o *observer) writeTxDecorator(tx dbt.WriteTx) dbt.WriteTx {
	return &txObserver{
		o:       o,
		WriteTx: tx,
	}
}

func (to *txObserver) Delete(path dbpath.Path) {
	to.WriteTx.Delete(path)

	to.changes = append(to.changes, dbt.ObservedChange{
		Path: path,
		Type: dbt.ChangeTypeDeleted,
	})
}

func (to *txObserver) CreateMap(path dbpath.Path) {
	to.WriteTx.CreateMap(path)

	to.changes = append(to.changes, dbt.ObservedChange{
		Path: path,
		Type: dbt.ChangeTypeMapCreated,
	})
}

func (to *txObserver) Put(path dbpath.Path, data []byte) {
	to.WriteTx.Put(path, data)

	to.changes = append(to.changes, dbt.ObservedChange{
		Path: path,
		Type: dbt.ChangeTypeValueSet,
	})

}

func (to *txObserver) OnCommit() {
	to.o.broadcastChanges(to.changes)
}

func (to *txObserver) OnRollback(err error) {
}

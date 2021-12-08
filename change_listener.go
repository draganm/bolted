package bolted

import "github.com/draganm/bolted/dbpath"

// ChangeListener will receive following callbacks during a write transaction:
type ChangeListener interface {
	Opened(b *Bolted) error
	Start(w Write) error
	Delete(w Write, path dbpath.Path) error
	CreateMap(w Write, path dbpath.Path) error
	Put(w Write, path dbpath.Path, newValue []byte) error
	BeforeCommit(w Write) error
	AfterTransaction(err error) error
	Closed() error
}

type CompositeChangeListener []ChangeListener

func (c CompositeChangeListener) Opened(b *Bolted) error {
	for _, cl := range c {
		err := cl.Opened(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Start(w Write) error {
	for _, cl := range c {
		err := cl.Start(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Delete(w Write, path dbpath.Path) error {
	for _, cl := range c {
		err := cl.Delete(w, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) CreateMap(w Write, path dbpath.Path) error {
	for _, cl := range c {
		err := cl.CreateMap(w, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Put(w Write, path dbpath.Path, newValue []byte) error {
	for _, cl := range c {
		err := cl.Put(w, path, newValue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) BeforeCommit(w Write) error {
	for _, cl := range c {
		err := cl.BeforeCommit(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) AfterTransaction(err error) error {
	for _, cl := range c {
		err := cl.AfterTransaction(err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Closed() error {
	for _, cl := range c {
		err := cl.Closed()
		if err != nil {
			return err
		}
	}
	return nil
}

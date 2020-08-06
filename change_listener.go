package bolted

// ChangeListener will receive following callbacks during a write transaction:
type ChangeListener interface {
	Start(w WriteTx) error
	Delete(w WriteTx, path string) error
	CreateMap(w WriteTx, path string) error
	Put(w WriteTx, path string, newValue []byte) error
	BeforeCommit(w WriteTx) error
	AfterTransaction(err error) error
}

type CompositeChangeListener []ChangeListener

func (c CompositeChangeListener) Start(w WriteTx) error {
	for _, cl := range c {
		err := cl.Start(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Delete(w WriteTx, path string) error {
	for _, cl := range c {
		err := cl.Delete(w, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) CreateMap(w WriteTx, path string) error {
	for _, cl := range c {
		err := cl.CreateMap(w, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) Put(w WriteTx, path string, newValue []byte) error {
	for _, cl := range c {
		err := cl.Put(w, path, newValue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CompositeChangeListener) BeforeCommit(w WriteTx) error {
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

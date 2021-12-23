package embedded

import "errors"

type Option func(*Bolted) error

func WithWriteTxDecorators(d ...WriteTxDecorator) func(*Bolted) error {
	return func(b *Bolted) error {
		for _, cl := range d {
			if cl == nil {
				return errors.New("writeTx decorator must not be nil")
			}
		}
		b.writeTxDecorators = append(b.writeTxDecorators, d...)
		return nil
	}
}

func WithNoSync() func(*Bolted) error {
	return func(b *Bolted) error {
		b.db.NoSync = true
		return nil
	}
}

func WithNoGrowSync() func(*Bolted) error {
	return func(b *Bolted) error {
		b.db.NoGrowSync = true
		return nil
	}
}

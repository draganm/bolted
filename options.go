package bolted

import "errors"

type Option func(*Bolted) error

func WithChangeListeners(c ...ChangeListener) func(*Bolted) error {
	return func(b *Bolted) error {
		for _, cl := range c {
			if cl == nil {
				return errors.New("change listener must not be nil")
			}
		}
		b.changeListeners = append(b.changeListeners, c...)
		return nil
	}
}

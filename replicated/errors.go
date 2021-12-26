package replicated

import "errors"

var ErrStale = errors.New("write transaction on stale data")

func IsStale(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrStale)
}

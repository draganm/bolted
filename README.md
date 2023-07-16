# Bolted

Wrapper around [bbolt](https://github.com/etcd-io/bbolt) database adding following features.

* More concise and readable code within the transactions, especially when using nested buckets.
* Observer pattern, allowing to be notified when values of interest did change within a transaction.
* Publishing [Prometheus](https://github.com/prometheus/client_golang) metrics
* [Open Telemetry](https://opentelemetry.io/) spans for `Read` and `Write` operations.

Bolted emphasizes use of the nested buckets in `bbolt` turning it into a highly performant filesystem with ACID transactions that is easy to use.

## Why should I use it?

Consider this code that will read a value and unmarshal JSON from two nested buckets:

```go
package example

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

func ReadUser(db *bbolt.DB) (*User, error) {
	var v []byte

	err := db.View(func(tx *bbolt.Tx) error {
		b1 := tx.Bucket([]byte("foo"))
		if b1 == nil {
			return errors.New("bucket 'foo' does not exist")
		}
		b2 := b1.Bucket([]byte("bar"))
		if b2 == nil {
			return errors.New("bucket 'foo/bar' does not exist")
		}
		vd := b2.Get([]byte("baz"))
		if vd == nil {
			return errors.New("value 'foo/bar/baz' does not exist")
		}
		v = make([]byte, len(vd))
		copy(v, vd)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read tx failed: %w", err)
	}

	u := &User{}
	err = json.Unmarshal(v, u)
	if err != nil {
		return nil, fmt.Errorf("could not parse the user: %w", err)
	}

	return u, nil

}
```

and compare it to the identical code using `bolted`:

```go
package example

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted"
)

func ReadUserBolted(db bolted.Database) (*User, error) {
	var v []byte

	err := db.Read(context.Background(), func(tx bolted.ReadTx) error {
		v = tx.Get(dbpath.ToPath("foo", "bar", "baz"))
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("read tx failed: %w", err)
	}

	u := &User{}
	err = json.Unmarshal(v, u)
	if err != nil {
		return nil, fmt.Errorf("could not parse the user: %w", err)
	}

	return u, nil

}

```

you will quickly notice that `bolted` will let you express the same semantic with 2 lines of code instead of 15.


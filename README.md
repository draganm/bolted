# Bolted

Bolted is a lightweight and easy-to-use wrapper around [bbolt](https://github.com/etcd-io/bbolt) database, providing additional features and a more concise API.
With Bolted, you can work with nested buckets within transactions in a more expressive and readable manner. 
It also offers an observer pattern for change notifications, supports Prometheus metrics for monitoring, and integrates Open Telemetry spans for tracing read and write operations.

# Features

* Concise Transactions: Express nested bucket operations with fewer lines of code, making your codebase more readable and maintainable.
* Observer Pattern: Receive notifications when values of interest change within a transaction, allowing you to respond to data updates effectively.
* [Prometheus Metrics](https://github.com/prometheus/client_golang): Monitor your database usage and performance with built-in support for publishing Prometheus metrics.
* [Open Telemetry](https://opentelemetry.io/) Spans: Gain insights into the performance of read and write operations using Open Telemetry spans.

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

## Contributing

We welcome contributions to Bolted! If you find any issues or have ideas for improvements, feel free to open an issue or submit a pull request on [GitHub](https://github.com/draganm/bolted).


## License
Bolted is distributed under the MIT License, making it free and open-source for anyone to use.

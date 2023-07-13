package local

import "github.com/draganm/bolted"

type WriteTxDecorator func(tx bolted.WriteTx) bolted.WriteTx
type CommitListener interface {
	OnCommit()
	OnRollback(err error)
}

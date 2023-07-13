package local

import "github.com/draganm/bolted/dbt"

type WriteTxDecorator func(tx dbt.WriteTx) dbt.WriteTx
type CommitListener interface {
	OnCommit()
	OnRollback(err error)
}

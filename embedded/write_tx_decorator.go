package embedded

import "github.com/draganm/bolted/database"

type WriteTxDecorator func(tx database.WriteTx) database.WriteTx

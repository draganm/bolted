package embedded

import "github.com/draganm/bolted"

type WriteTxDecorator func(tx bolted.WriteTx) bolted.WriteTx

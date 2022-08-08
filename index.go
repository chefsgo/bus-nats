package bus_nats

import (
	"github.com/chefsgo/bus"
)

func Driver() bus.Driver {
	return &natsBusDriver{}
}

func init() {
	bus.Register("nats", Driver())
}

package tcr

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

// A closed deliveryChan must make processDeliveries return, not spin
// manufacturing zero-value deliveries at CPU speed (EAI-1172).
func Test_processDeliveries_returnsWhenDeliveryChannelCloses(t *testing.T) {
	con := &Consumer{
		ConnectionPool:   &ConnectionPool{},
		receivedMessages: make(chan *ReceivedMessage, 1000),
		consumeStop:      make(chan bool, 1),
		errors:           make(chan error, 1000),
		conLock:          &sync.Mutex{},
	}
	chanHost := &ChannelHost{Errors: make(chan *amqp.Error, 100)}

	deliveryChan := make(chan amqp.Delivery)
	close(deliveryChan) // simulates RabbitMQ NotifyClose dropping the connection

	var produced int64
	stopDrain := make(chan struct{})
	go func() {
		for {
			select {
			case <-con.receivedMessages:
				atomic.AddInt64(&produced, 1)
			case <-stopDrain:
				return
			}
		}
	}()

	returned := make(chan bool, 1)
	go func() {
		returned <- con.processDeliveries(deliveryChan, chanHost, nil)
	}()

	select {
	case <-returned:
		close(stopDrain)
		t.Logf("processDeliveries returned correctly after %d manufactured messages", atomic.LoadInt64(&produced))
	case <-time.After(1 * time.Second):
		close(stopDrain)
		t.Fatalf(
			"processDeliveries did not return within 1s of the delivery channel closing (EAI-1172): "+
				"it manufactured %d zero-value deliveries instead of detecting the closed channel and returning to redial",
			atomic.LoadInt64(&produced),
		)
	}
}

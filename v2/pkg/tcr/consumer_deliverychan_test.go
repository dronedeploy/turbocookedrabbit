package tcr

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

// Test_processDeliveries_returnsWhenDeliveryChannelCloses reproduces EAI-1172.
//
// When RabbitMQ drops the connection (e.g. CONNECTION_FORCED on node restart),
// the raw amqp.Delivery channel handed to processDeliveries is closed. The
// receive at "case delivery := <-deliveryChan" has no ok check, so once the
// channel is closed every read returns a zero-value amqp.Delivery immediately
// (a closed channel receive never blocks), and processDeliveries never falls
// through to the default branch's idle sleep. This manufactures a non-nil
// ReceivedMessage with an empty body on every iteration, pushed to
// receivedMessages as fast as it can be drained - producing the CPU/memory
// flood that OOMKilled stormie in prod, stage, and test on 2026-08-02.
//
// The fix is to check the second (ok) value on the deliveryChan receive and
// return false (same as the chanHost.Errors path) so startConsumeLoop redials
// a fresh channel instead of spinning on the dead one.
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

//go:build integration
// +build integration

package msgbuzz_test

import (
	"os"
	"testing"
	"time"

	"github.com/sihendra/go-msgbuzz"
	"github.com/stretchr/testify/require"
)

// TODO improve testing
func TestRabbitMqMessageConfirm_Retry(t *testing.T) {

	t.Run("ShouldRetry", func(t *testing.T) {
		mc, errClient := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), msgbuzz.WithConsumerThread(1))
		require.NoError(t, errClient)
		defer mc.Close()
		topicName := "msgconfirm_retry_test"
		consumerName := "msgconfirm_test"
		doneChan := make(chan bool)

		maxRetry := 3
		expectedRetryCount := 1
		expectedMaxAttempt := expectedRetryCount + 1 // retry + original msg
		delaySecond := 1
		var actualAttempt int
		err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
			actualAttempt++
			if shouldRetry := actualAttempt < expectedMaxAttempt; shouldRetry {
				// CODE UNDER TEST
				err := confirm.Retry(int64(delaySecond), maxRetry)
				require.NoError(t, err)
				return nil
			}

			confirm.Ack()
			doneChan <- true
			return nil
		})
		require.NoError(t, err)
		go mc.StartConsuming()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		err = mc.Publish(topicName, []byte("something"))
		require.NoError(t, err)

		// wait until timeout or done
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Timeout after %d seconds", waitSec)
		case <-doneChan:
			// Should retry n times
			require.Equal(t, expectedMaxAttempt, actualAttempt)
		}
	})

	t.Run("ShouldReturnError_WhenMaxRetryReached", func(t *testing.T) {
		mc, errClient := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), msgbuzz.WithConsumerThread(1))
		require.NoError(t, errClient)
		defer mc.Close()
		topicName := "msgconfirm_retry_max_test"
		consumerName := "msgconfirm_test"
		doneChan := make(chan bool)

		maxRetry := 2
		expectedRetryCount := maxRetry
		expectedMaxAttempt := 1 + expectedRetryCount // original msg + retry
		delayMs := 1000
		var errRetry error
		var actualAttempt int
		err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
			actualAttempt++
			t.Logf("Attempt: %d", actualAttempt)

			// Retry
			errRetry = confirm.Retry(int64(delayMs), int(maxRetry))
			if errRetry != nil {
				doneChan <- true
			}

			return nil
		})
		require.NoError(t, err)
		go mc.StartConsuming()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		err = mc.Publish(topicName, []byte("something"))
		require.NoError(t, err)

		// wait until timeout or done
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Timeout after %d seconds", waitSec)
		case <-doneChan:
		}

		// -- WhenMaxRetryReached
		// -- Should execute 1 + max retries
		require.Equal(t, expectedMaxAttempt, actualAttempt)
		// -- Should retry n times
		require.Equal(t, expectedRetryCount, actualAttempt-1)
		// -- ShouldReturnError on max retry + 1
		require.Error(t, errRetry)
		require.Equal(t, "max retry reached", errRetry.Error())
	})

}

//go:build integration
// +build integration

package msgbuzz

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRabbitMqClient_Publish(t *testing.T) {
	t.Skip()
	t.Run("ShouldPublishMessageToTopic", func(t *testing.T) {
		// Init
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)
		testTopicName := "msgbuzz.pubtest"
		actualMsgReceivedChan := make(chan []byte)

		// -- listen topic to check published message
		rabbitClient.On(testTopicName, "msgbuzz", func(confirm MessageConfirm, bytes []byte) error {
			actualMsgReceivedChan <- bytes
			return confirm.Ack()
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// -- wait for exchange and queue to be created
		time.Sleep(1 * time.Second)

		// Code under test
		sentMessage := []byte("some msg from msgbuzz")
		err := rabbitClient.Publish(testTopicName, sentMessage)

		// Expectations
		// -- ShouldPublishMessageToTopic
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}
	})

	t.Run("ShouldPublishMessageToTopicWithRoutingKeys", func(t *testing.T) {
		// Init
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)

		testTopicName := "msgbuzz.pubtest.routing"
		actualMsgReceivedChan := make(chan []byte)
		routingKey := "routing_key"

		ch, err := rabbitClient.conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		// Declare a direct exchange
		err = ch.ExchangeDeclare(
			testTopicName,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// Declare queue to the exchange
		q, err := ch.QueueDeclare(
			testTopicName,
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// Bind a queue to the exchange with the routing key
		err = ch.QueueBind(
			q.Name,
			routingKey,
			testTopicName,
			false,
			nil,
		)
		require.NoError(t, err)

		// Consume messages from the queue
		msgs, err := ch.Consume(
			q.Name,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// -- listen topic to check published message
		go func() {
			for msg := range msgs {
				actualMsgReceivedChan <- msg.Body
			}
		}()

		defer rabbitClient.Close()

		// -- wait for exchange and queue to be created
		time.Sleep(3 * time.Second)

		// Code under test
		sentMessage := []byte("some msg from msgbuzz with routing keys")
		err = rabbitClient.Publish(testTopicName, sentMessage, WithRabbitMqRoutingKey(routingKey))

		// Expectations
		// -- ShouldPublishMessageToTopic
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}
	})

	t.Run("ShouldReconnectAndPublishToTopic_WhenDisconnectedFromRabbitMqServer", func(t *testing.T) {
		// Init
		err := StartRabbitMqServer()
		require.NoError(t, err)

		// Create rabbitmq client, this WILL CREATE CONNECTION for consumer and publisher
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)

		topicName := "msgbuzz.reconnect.test"
		consumerName := "msgbuzz"
		actualMsgSent := make(chan bool)

		rabbitClient.On(topicName, consumerName, func(confirm MessageConfirm, bytes []byte) error {
			defer func() {
				require.NoError(t, confirm.Ack())
			}()
			actualMsgSent <- true
			require.Equal(t, "Hi from msgbuzz", string(bytes))
			return nil
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// restart RabbitMQ dummy server
		err = RestartRabbitMqServer()
		require.NoError(t, err)

		// RESTART WILL TRIGGER consumer and publisher reconnection
		// wait until consumer reconnecting finish
		time.Sleep(8 * time.Second)

		err = rabbitClient.Publish(topicName, []byte("Hi from msgbuzz"))

		// Expectations
		// -- Should publish message
		require.NoError(t, err)

		// -- Should receive message
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving message after %d seconds", waitSec)
		case msgSent := <-actualMsgSent:
			require.True(t, msgSent)
		}
	})

	t.Run("ShouldPublishSuccessfully_WhenChannelCleanerRun", func(t *testing.T) {
		// Init
		maxIdle := 100 * time.Millisecond
		maxChannel := 20
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"),
			WithConsumerThread(1),
			WithPubMinChannel(1),
			WithPubMaxChannel(maxChannel),
			WithPubChannelPruneInterval(maxIdle/10), //
			WithPubChannelMaxIdle(maxIdle),
		)
		require.NoError(t, errClient)

		topicName := "msgbuzz.channelprune.test"
		consumerName := "msgbuzz"

		// Code under test
		rabbitClient.On(topicName, consumerName, func(confirm MessageConfirm, bytes []byte) error {
			defer confirm.Ack()
			return nil
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		// Publish to warmup the channel
		for i := 0; i < maxChannel; i++ {
			err := rabbitClient.Publish(topicName, []byte("Hi from msgbuzz"))
			// -- Should publish without error
			require.NoError(t, err)
		}
		// Wait until channel expire and cleanup run
		<-time.After(maxIdle)
		for i := 0; i < maxChannel*100; i++ {
			err := rabbitClient.Publish(topicName, []byte("Hi from msgbuzz"))
			// Expectations
			// -- Should publish without error
			require.NoError(t, err)
		}
	})
}

func TestRabbitMqClient_PublishWithContext(t *testing.T) {

	t.Run("ShouldPublishMessageToTopic", func(t *testing.T) {
		// Init
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)
		testTopicName := "msgbuzz.pubtest"
		actualMsgReceivedChan := make(chan []byte)

		// -- listen topic to check published message
		rabbitClient.On(testTopicName, "msgbuzz", func(confirm MessageConfirm, bytes []byte) error {
			actualMsgReceivedChan <- bytes
			return confirm.Ack()
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// -- wait for exchange and queue to be created
		time.Sleep(1 * time.Second)

		// Code under test
		sentMessage := []byte("some msg from msgbuzz")
		err := rabbitClient.PublishWithContext(context.Background(), testTopicName, sentMessage)

		// Expectations
		// -- ShouldPublishMessageToTopic
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}
	})

	t.Run("ShouldTimeout_WhenGivenContextWithTimeoutAndConnectingTimeout", func(t *testing.T) {
		// Init
		// Create rabbitmq client, this WILL CREATE CONNECTION for consumer and publisher
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"),
			WithConsumerThread(1),
			// -- WhenGivenContextWithTimeoutAndConnectingTimeout
			// this will create no publisher, thus timeout when timeout exceeded
			WithPubMinChannel(0),
			WithPubMaxChannel(0))
		require.NoError(t, errClient)

		topicName := "msgbuzz.reconnect.test"
		consumerName := "msgbuzz"
		actualMsgSent := make(chan bool)

		rabbitClient.On(topicName, consumerName, func(confirm MessageConfirm, bytes []byte) error {
			defer func() {
				require.NoError(t, confirm.Ack())
			}()
			actualMsgSent <- true
			require.Equal(t, "Hi from msgbuzz", string(bytes))
			return nil
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// restart RabbitMQ dummy server
		//err = stopRabbitMqServer()
		//require.NoError(t, err)

		// Publish on closed rabbitmq
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		err := rabbitClient.PublishWithContext(ctx, topicName, []byte("Hi from msgbuzz"))

		// Expectations
		// -- Should timeout
		require.ErrorIs(t, err, context.DeadlineExceeded)

	})

}

func TestRabbitMqClient_Close(t *testing.T) {
	t.Run("ShouldReturnNoError_WhenCloseCalledTwice", func(t *testing.T) {
		// Init
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)

		// Code under test
		err := rabbitClient.Close()
		// Expectations
		// -- ShouldRetunNoError
		require.NoError(t, err)

		// Code under test
		err = rabbitClient.Close()

		// -- ShouldRetunNoError when call Close twice
		require.NoError(t, err)

	})

	t.Run("ShouldWaitUntilConsumersFinish", func(t *testing.T) {
		// Init
		rabbitClient, errClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), WithConsumerThread(1))
		require.NoError(t, errClient)

		testTopicName := "msgbuzz.closetest"
		actualMsgReceivedChan := make(chan []byte)

		// -- listen topic
		rabbitClient.On(testTopicName, "msgbuzz", func(confirm MessageConfirm, bytes []byte) error {
			defer func() {
				require.NoError(t, confirm.Ack())
			}()

			// wait consumer finish
			time.Sleep(2 * time.Second)

			actualMsgReceivedChan <- bytes

			// publish random topic
			err := rabbitClient.Publish("msgbuzz.closetest-random", []byte("Hi from msgbuzz"))
			// -- ShouldRetunNoError
			require.NoError(t, err)

			return nil
		})

		go rabbitClient.StartConsuming()
		// -- wait for exchange and queue to be created
		time.Sleep(1 * time.Second)

		//publish topic
		sentMessage := []byte("some msg from msgbuzz")
		err := rabbitClient.Publish(testTopicName, sentMessage)
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}

		// Code under test
		err = rabbitClient.Close()

		// Expectations
		// -- ShouldRetunNoError
		require.NoError(t, err)

	})
}

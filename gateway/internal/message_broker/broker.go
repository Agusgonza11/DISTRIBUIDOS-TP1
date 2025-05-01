package message_broker

import (
	"fmt"
	"tp1-sistemas-distribuidos/gateway/internal/config"

	"github.com/op/go-logging"
	"github.com/streadway/amqp"
)

type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *logging.Logger
}

func NewBroker(config config.RabbitMQConfig, logger *logging.Logger) (*Broker, error) {
	conn, err := amqp.Dial(config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	queueNames := getQueueNamesFromConfig(config)

	for _, queueName := range queueNames {
		_, err := channel.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to declare queue (%s): %w", queueName, err)
		}
	}
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Broker{
		conn:    conn,
		channel: channel,
		logger:  logger,
	}, nil
}

func (b *Broker) PublishMessage(queueName string, headers map[string]interface{}, body []byte) error {
	err := b.channel.Publish(
		"",
		queueName,
		true,
		false,
		amqp.Publishing{
			Headers:     headers,
			ContentType: "text/plain; charset=utf-8",
			Body:        body,
		},
	)
	if err != nil {
		b.logger.Errorf("failed to publish message to queue %s: %v", queueName, err)
	}
	return err
}
func (b *Broker) Consume(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := b.channel.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		b.logger.Errorf("failed to consume RabbitMQ queue: %v", err)
		return nil, err
	}

	return msgs, nil
}

func (b *Broker) Close() {
	b.channel.Close()
	b.conn.Close()
}

func getQueueNamesFromConfig(config config.RabbitMQConfig) []string {
	var queueNames []string

	for _, queue := range config.FilterQueues {
		queueNames = append(queueNames, queue)
	}

	for _, queue := range config.JoinQueues {
		queueNames = append(queueNames, queue)
	}

	queueNames = append(queueNames, config.OutputQueueName)

	return queueNames
}

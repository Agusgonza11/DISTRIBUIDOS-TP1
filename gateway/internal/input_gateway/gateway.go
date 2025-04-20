package input_gateway

import (
	"context"
	"fmt"
	"net"
	"sync"
	"tp1-sistemas-distribuidos/gateway/internal/config"

	"github.com/op/go-logging"
	"github.com/streadway/amqp"
)

type Gateway struct {
	config  config.InputGatewayConfig
	running bool
	// TODO agregar running mutex
	amqpChannel *amqp.Channel
	logger      *logging.Logger
}

func NewGateway(config config.InputGatewayConfig, logger *logging.Logger) (*Gateway, error) {
	conn, err := amqp.Dial(config.RabbitMQ.Address)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed trying to open a channel: %w", err)
	}

	for _, queueName := range config.RabbitMQ.FilterQueues {
		_, err := channel.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to declare a queue (%s): %w", queueName, err)
		}
	}

	return &Gateway{
		config:      config,
		amqpChannel: channel,
		running:     true,
		logger:      logger,
	}, nil
}

func (g *Gateway) Start(ctx context.Context) {
	addresses := map[string]struct {
		address     string
		handlerFunc func(conn net.Conn)
	}{
		"movies": {
			address:     g.config.MoviesAddress,
			handlerFunc: g.moviesHandler,
		},
		"credits": {
			address:     g.config.CreditsAddress,
			handlerFunc: g.moviesHandler,
		},
		"ratings": {
			address:     g.config.RatingsAddress,
			handlerFunc: g.moviesHandler,
		},
	}

	wg := sync.WaitGroup{}

	for key, data := range addresses {
		wg.Add(1)

		listener, err := net.Listen("tcp", data.address)
		if err != nil {
			g.logger.Errorf("failed to start %s listener: %v", key, err)
			continue
		}

		go func(listener net.Listener) {
			defer listener.Close()
			defer wg.Done()

			go func() {
				g.gracefulShutdown(ctx, listener)
			}()

			g.acceptConnections(listener, data.handlerFunc)
		}(listener)
	}

	wg.Wait()
}

func (g *Gateway) acceptConnections(listener net.Listener, handlerFunc func(net.Conn)) {
	for g.running {
		conn, err := listener.Accept()
		if err != nil {
			if g.running {
				g.logger.Errorf("failed to accept connection: %v", err)
			}

			continue
		}
		go handlerFunc(conn)
	}
}

func (g *Gateway) gracefulShutdown(ctx context.Context, listener net.Listener) {
	<-ctx.Done()
	listener.Close()
	g.running = false
}

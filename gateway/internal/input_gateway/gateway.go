package input_gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"tp1-sistemas-distribuidos/gateway/internal/config"
	"tp1-sistemas-distribuidos/gateway/internal/models"
	"tp1-sistemas-distribuidos/gateway/internal/utils"

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
		address            string
		messageBuilderFunc func([]string) ([]byte, error)
	}{
		"movies": {
			address:            g.config.MoviesAddress,
			messageBuilderFunc: g.buildMoviesMessage,
		},
		"credits": {
			address:            g.config.CreditsAddress,
			messageBuilderFunc: g.buildCreditsMessage,
		},
		"ratings": {
			address:            g.config.RatingsAddress,
			messageBuilderFunc: g.buildRatingsMessage,
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

			g.acceptConnections(listener, data.messageBuilderFunc)
		}(listener)
	}

	wg.Wait()
}

func (g *Gateway) handleMessage(
	conn net.Conn,
	messageBuilderFunc func([]string) ([]byte, error),
) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for g.running {
		response, err := utils.ReadMessage(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				g.logger.Errorf(fmt.Sprintf("error reading message: %v", err))
			}
			return
		}

		lines := strings.Split(response, "\n")
		if len(lines) < 1 {
			continue
		}

		header := lines[0]
		splittedHeader := strings.Split(header, ",")

		messageType := strings.TrimSpace(splittedHeader[0])

		if messageType == models.MessageEOF {
			g.logger.Infof("EOF_ACK sent")
			err := utils.WriteMessage(conn, []byte("EOF_ACK"))
			if err != nil {
				g.logger.Errorf("failed trying to eof ack: %v", err)
				return
			}

			return
		}

		batchID := splittedHeader[3]
		g.logger.Infof(fmt.Sprintf("%s_ACK:%s", batchID, strings.ToUpper(splittedHeader[1])))

		err = utils.WriteMessage(conn, []byte(fmt.Sprintf("%s_ACK:%s", strings.ToUpper(splittedHeader[1]), batchID)))
		if err != nil {
			g.logger.Errorf("failed trying to send movies ack: %v", err)
			return
		}

		queueName, exists := g.getQueueNameByQuery(messageType, splittedHeader[1])
		if !exists {
			g.logger.Errorf("QUEUE NOT FOUND")
			return
		}

		body, err := messageBuilderFunc(lines[1:])
		if err != nil {
			g.logger.Errorf("error trying to build message: %v", err)
			return
		}

		err = g.amqpChannel.Publish(
			"",
			queueName,
			true,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"Query":     messageType,
					"Client-ID": splittedHeader[2],
				},
				ContentType: "text/plain; charset=utf-8",
				Body:        body,
			},
		)
		if err != nil {
			g.logger.Errorf("failed trying to publish message: %v", err)
			return
		}
	}
}

func (g *Gateway) getQueueNameByQuery(query string, file string) (string, bool) {
	switch file {
	case "movies":
		switch query {
		case QueryArgentinaEsp, QueryTopInvestors,
			QueryTopArgentinianMoviesByRating,
			QueryTopArgentinianActors, QuerySentimentAnalysis:
			queueName, found := g.config.RabbitMQ.FilterQueues[query]
			return queueName, found
		default:
			return "", false
		}
	case "credits":
		switch query {
		case QueryTopArgentinianActors:
			queueName, found := g.config.RabbitMQ.JoinQueues[query]
			return queueName, found
		default:
			return "", false
		}
	case "ratings":
		switch query {
		case QueryTopArgentinianMoviesByRating:
			queueName, found := g.config.RabbitMQ.JoinQueues[query]
			return queueName, found
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func (g *Gateway) acceptConnections(listener net.Listener, messageBuilderFunc func([]string) ([]byte, error)) {
	for g.running {
		conn, err := listener.Accept()
		if err != nil {
			if g.running {
				g.logger.Errorf("failed to accept connection: %v", err)
			}

			continue
		}
		go g.handleMessage(conn, messageBuilderFunc)
	}
}

func (g *Gateway) gracefulShutdown(ctx context.Context, listener net.Listener) {
	<-ctx.Done()
	listener.Close()
	g.running = false
}

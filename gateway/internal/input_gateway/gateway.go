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
		messageBuilderFunc func([]string, string) ([]byte, error)
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
	messageBuilderFunc func([]string, string) ([]byte, error),
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
		file := splittedHeader[1]
		clientID := splittedHeader[2]
		batchID := splittedHeader[3]

		queueName, exists := g.getQueueNameByQuery(messageType, file)
		if !exists {
			g.logger.Errorf("QUEUE NOT FOUND: message_type: %s, file: %s", messageType, file)
			return
		}

		isEOF := splittedHeader[len(splittedHeader)-1] == models.MessageEOF
		if isEOF {
			g.handleEOFMessage(conn, queueName, messageType, file, clientID)
		} else {
			g.handleCommonMessage(conn, queueName, messageType, file, batchID, clientID, lines, messageBuilderFunc)
		}
	}
}

func (g *Gateway) handleCommonMessage(
	conn net.Conn,
	queueName, query, file, batchID, clientID string,
	lines []string,
	messageBuilderFunc func([]string, string) ([]byte, error),
) {
	g.logger.Infof(fmt.Sprintf("%s_ACK:%s", batchID, file))

	err := utils.WriteMessage(conn, []byte(fmt.Sprintf("%s_ACK:%s", file, batchID)))
	if err != nil {
		g.logger.Errorf("failed trying to send movies ack: %v", err)
		return
	}

	body, err := messageBuilderFunc(lines[1:], query)
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
				"Query":    query,
				"ClientID": clientID,
				"type": file,
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

func (g *Gateway) handleEOFMessage(conn net.Conn, queueName, query, file, clientID string) {
	g.logger.Infof("EOF_ACK sent")
	err := utils.WriteMessage(conn, []byte("EOF_ACK"))
	if err != nil {
		g.logger.Errorf("failed trying to eof ack: %v", err)
		return
	}

	messagesToSend := g.getEOFCountByQuery(query, file)

	eofHeader := g.getEOFHeaderByQuery(query, file)
	g.logger.Infof("Sending %d EOF's: %s to %s queue", messagesToSend, eofHeader, queueName)

	for range messagesToSend {
		err = g.amqpChannel.Publish(
			"",
			queueName,
			true,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"Query":    query,
					"ClientID": clientID,
					"type":     eofHeader,
				},
				ContentType: "text/plain; charset=utf-8",
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
	case "MOVIES":
		switch query {
		case QueryArgentinaEsp, QueryTopInvestors,
			QueryTopArgentinianMoviesByRating,
			QueryTopArgentinianActors, QuerySentimentAnalysis:
			queueName, found := g.config.RabbitMQ.FilterQueues[query]
			return queueName, found
		default:
			return "", false
		}
	case "CREDITS":
		switch query {
		case QueryTopArgentinianActors:
			queueName, found := g.config.RabbitMQ.JoinQueues[query]
			return queueName, found
		default:
			return "", false
		}
	case "RATINGS":
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

func (g *Gateway) getEOFCountByQuery(query string, file string) int {
	switch file {
	case "MOVIES":
		switch query {
		case QueryArgentinaEsp:
			return g.config.EOFsCount["CONSULTA_1_FILTER"]
		case QueryTopInvestors:
			return g.config.EOFsCount["CONSULTA_2_FILTER"]
		case QueryTopArgentinianMoviesByRating:
			return g.config.EOFsCount["CONSULTA_3_FILTER"]
		case QueryTopArgentinianActors:
			return g.config.EOFsCount["CONSULTA_4_FILTER"]
		case QuerySentimentAnalysis:
			return g.config.EOFsCount["CONSULTA_5_FILTER"]
		default:
			return 0
		}
	case "RATINGS":
		return g.config.EOFsCount["CONSULTA_3_JOIN"]
	case "CREDITS":
		return g.config.EOFsCount["CONSULTA_4_JOIN"]
	default:
		return 0
	}
}

func (g *Gateway) getEOFHeaderByQuery(query string, file string) string {
	switch file {
	case "MOVIES":
		switch query {
		case QueryArgentinaEsp, QueryTopInvestors,
			QueryTopArgentinianMoviesByRating,
			QueryTopArgentinianActors, QuerySentimentAnalysis:
			return models.MessageEOF
		default:
			return ""
		}
	case "CREDITS":
		switch query {
		case QueryTopArgentinianActors:
			return models.MessageEOFCredits
		default:
			return ""
		}
	case "RATINGS":
		switch query {
		case QueryTopArgentinianMoviesByRating:
			return models.MessageEOFRatings
		default:
			return ""
		}
	default:
		return ""
	}
}

func (g *Gateway) acceptConnections(listener net.Listener, messageBuilderFunc func([]string, string) ([]byte, error)) {
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

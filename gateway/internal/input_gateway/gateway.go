package input_gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	goIO "io"
	"net"
	"strings"
	"sync"

	"github.com/op/go-logging"

	"tp1-sistemas-distribuidos/gateway/internal/config"
	"tp1-sistemas-distribuidos/gateway/internal/models"
	io "tp1-sistemas-distribuidos/gateway/internal/utils"
)

type Broker interface {
	PublishMessage(queueName string, headers map[string]interface{}, body []byte) error
	Close()
}

type Gateway struct {
	config       config.InputGatewayConfig
	running      bool
	runningMutex sync.RWMutex
	broker       Broker
	logger       *logging.Logger
}

func NewGateway(broker Broker, config config.InputGatewayConfig, logger *logging.Logger) (*Gateway, error) {
	return &Gateway{
		config:  config,
		broker:  broker,
		running: true,
		logger:  logger,
	}, nil
}

func (g *Gateway) listenForConnections(ctx context.Context) {
	listener, err := net.Listen("tcp", g.config.ConnectionsAddress)
	if err != nil {
		g.logger.Errorf("failed to start connections listener: %v", err)
		return
	}
	defer listener.Close()

	go func() {
		g.gracefulShutdown(ctx, listener)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			g.logger.Errorf("failed to accept connection in connections listener: %v", err)
			continue
		}

		go g.handleConnectionRequest(conn)
	}
}

func (g *Gateway) handleConnectionRequest(conn net.Conn) {
	defer conn.Close()

	clientID := uuid.New().String()

	err := io.WriteMessage(conn, []byte(clientID))
	if err != nil {
		g.logger.Errorf("failed to send id to client: %v", err)
		return
	}

	g.logger.Infof("assigned id %s to new client", clientID)
}

func (g *Gateway) Start(ctx context.Context) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		g.listenForConnections(ctx)
	}()

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

func (g *Gateway) acceptConnections(listener net.Listener, messageBuilderFunc func([]string, string) ([]byte, error)) {
	for g.isRunning() {
		conn, err := listener.Accept()
		if err != nil {
			if g.isRunning() {
				g.logger.Errorf("failed to accept connection: %v", err)
			}

			continue
		}

		go g.handleMessage(conn, messageBuilderFunc)
	}
}

func (g *Gateway) handleMessage(
	conn net.Conn,
	messageBuilderFunc func([]string, string) ([]byte, error),
) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for g.isRunning() {
		response, err := io.ReadMessage(reader)
		if err != nil {
			if !errors.Is(err, goIO.EOF) {
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
		rawQueries := strings.TrimSpace(splittedHeader[0])
		file := splittedHeader[1]
		clientID := splittedHeader[2]
		batchID := splittedHeader[3]
		isEOF := splittedHeader[len(splittedHeader)-1] == models.MessageEOF

		var queries []string
		if file == "MOVIES" {
			queries = strings.Split(rawQueries, "|")
		} else {
			queries = []string{rawQueries}
		}

		if isEOF {
			g.handleEOFMessage(conn, queries, file, clientID)
		} else {
			g.handleCommonMessage(conn, queries, file, batchID, clientID, lines, messageBuilderFunc)
		}
	}
}
func (g *Gateway) assignIDToClient(conn net.Conn) string {
	clientID := uuid.New()

	err := io.WriteMessage(conn, []byte(clientID.String()))
	if err != nil {
		errMessage := fmt.Sprintf("error sending id to client: %v", err)
		g.logger.Errorf(errMessage)
		return ""
	}

	return clientID.String()
}

func (g *Gateway) handleCommonMessage(
	conn net.Conn,
	queries []string,
	file, batchID, clientID string,
	lines []string,
	messageBuilderFunc func([]string, string) ([]byte, error),
) {
	//g.logger.Infof(fmt.Sprintf("%s_ACK:%s", file, batchID))

	err := io.WriteMessage(conn, []byte(fmt.Sprintf("%s_ACK:%s", file, batchID)))
	if err != nil {
		g.logger.Errorf("failed trying to send movies ack: %v", err)
		return
	}

	for _, query := range queries {

		queueName, exists := g.getQueueNameByQuery(query, file)
		if !exists {
			g.logger.Errorf("queue not found: message_type: %s, file: %s", query, file)
			continue
		}

		body, err := messageBuilderFunc(lines[1:], query)
		if err != nil {
			g.logger.Errorf("error trying to build message: %v", err)
			continue
		}

		err = g.broker.PublishMessage(
			queueName,
			map[string]interface{}{
				"Query":    query,
				"ClientID": clientID,
				"MessageID":  batchID,
				"BatchID":  batchID,
				"type":     file,
			},
			body,
		)
		if err != nil {
			g.logger.Errorf("failed trying to publish message: %v", err)
			continue
		}
	}
}

func (g *Gateway) handleEOFMessage(conn net.Conn, queries []string, file, clientID string) {
	eofACK := fmt.Sprintf("%s_EOF_ACK", file)

	g.logger.Infof("%s sent", eofACK)

	err := io.WriteMessage(conn, []byte(eofACK))
	if err != nil {
		g.logger.Errorf("failed trying to eof ack: %v", err)
		return
	}

	for _, query := range queries {

		messagesToSend := g.getEOFCountByQuery(query, file)
		eofHeader := g.getEOFHeaderByQuery(query, file)
		queueName, exists := g.getQueueNameByQuery(query, file)
		if !exists {
			g.logger.Errorf("queue not found: message_type: %s, file: %s", query, file)
			continue
		}

		g.logger.Infof("sending %d EOF's: %s to %s queue", messagesToSend, eofHeader, queueName)

		for i := 0; i < messagesToSend; i++ {
			err = g.broker.PublishMessage(
				queueName,
				map[string]interface{}{
					"Query":    query,
					"ClientID": clientID,
					"type":     eofHeader,
				},
				nil,
			)
			if err != nil {
				g.logger.Errorf("failed trying to publish message: %v", err)
				break
			}
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
		return 1
	case "CREDITS":
		return 1
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

func (g *Gateway) gracefulShutdown(ctx context.Context, listener net.Listener) {
	<-ctx.Done()
	listener.Close()
	g.broker.Close()
	g.stopRunning()
}

func (g *Gateway) isRunning() bool {
	g.runningMutex.RLock()
	defer g.runningMutex.RUnlock()
	return g.running
}

func (g *Gateway) stopRunning() {
	g.runningMutex.Lock()
	defer g.runningMutex.Unlock()
	g.running = false
}

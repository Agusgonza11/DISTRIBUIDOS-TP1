package output_gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	goIO "io"
	"net"
	"strings"
	"sync"
	"tp1-sistemas-distribuidos/gateway/internal/config"
	io "tp1-sistemas-distribuidos/gateway/internal/utils"

	"github.com/op/go-logging"
	"github.com/streadway/amqp"
)

type Gateway struct {
	config       config.OutputGatewayConfig
	amqpChannel  *amqp.Channel
	clients      map[string]net.Conn
	clientsMutex sync.RWMutex
	running      bool
	runningMutex sync.RWMutex
	logger       *logging.Logger
}

func NewGateway(config config.OutputGatewayConfig, logger *logging.Logger) (*Gateway, error) {
	conn, err := amqp.Dial(config.RabbitMQ.Address)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed trying to open a channel: %w", err)
	}

	_, err = channel.QueueDeclare(
		config.RabbitMQ.OutputQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare output queue: %w", err)
	}

	return &Gateway{
		config:      config,
		amqpChannel: channel,
		clients:     make(map[string]net.Conn),
		running:     true,
		logger:      logger,
	}, nil
}

func (g *Gateway) Start(ctx context.Context) {
	wg := sync.WaitGroup{}

	wg.Add(1)

	listener, err := net.Listen("tcp", g.config.Address)
	if err != nil {
		g.logger.Errorf("failed to start listener: %v", err)
		return
	}

	g.logger.Infof("starting to listen in port: %s", g.config.Address)

	go func() {
		defer listener.Close()
		defer wg.Done()

		go g.gracefulShutdown(ctx, listener)
		g.acceptConnections(listener)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		g.listenRabbitMQ(ctx)
	}()

	wg.Wait()
}

func (g *Gateway) acceptConnections(listener net.Listener) {
	for g.isRunning() {
		g.logger.Info("Waiting for conns")
		conn, err := listener.Accept()
		if err != nil {
			if g.isRunning() {
				g.logger.Errorf("failed to accept connection: %v", err)
			}
			continue
		}

		g.logger.Info("Connection accepted")

		go g.handleConnection(conn)
	}
}

func (g *Gateway) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	var clientID string

	response, err := io.ReadMessage(reader)
	if err != nil {
		if !errors.Is(err, goIO.EOF) {
			g.logger.Errorf("error reading message: %v", err)
		}

		g.logger.Infof("connection closed: %s", err.Error())
		return
	}

	g.logger.Infof("Message read: %s", response)

	lines := strings.Split(response, "\n")
	if len(lines) != 1 {
		g.logger.Info("malformed message")
		return
	}

	parts := strings.Split(lines[0], ",")
	if len(parts) < 2 {
		g.logger.Info("invalid format, expected id in message")
		return
	}

	clientID = strings.TrimSpace(parts[1])

	g.clientsMutex.Lock()
	g.clients[clientID] = conn
	g.clientsMutex.Unlock()

	g.logger.Infof("Client connected: %s", clientID)
}

func (g *Gateway) listenRabbitMQ(ctx context.Context) {
	msgs, err := g.amqpChannel.Consume(
		g.config.RabbitMQ.OutputQueueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		g.logger.Errorf("failed to consume RabbitMQ queue: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			g.logger.Info("stopping RabbitMQ listener")
			return
		case msg, ok := <-msgs:
			if !ok {
				g.logger.Warning("rabbitMQ channel closed")
				return
			}

			clientID := msg.Headers["ClientID"].(string)

			message := fmt.Sprintf("%s\n%s", msg.Headers["Query"], string(msg.Body))

			g.clientsMutex.RLock()
			conn, exists := g.clients[clientID]
			g.clientsMutex.RUnlock()

			if !exists {
				g.logger.Warningf("client %s not connected", clientID)
				continue
			}

			err := io.WriteMessage(conn, []byte(message))
			if err != nil {
				g.logger.Errorf("failed to send message to %s: %v", clientID, err)
				continue
			}

			reader := bufio.NewReader(conn)

			response, err := io.ReadMessage(reader)
			if err != nil {
				g.logger.Errorf("failed trying to read result ack for client id: %s, err: %v", clientID, err)
				continue
			}

			if response == "RESULT_ACK" {
				g.logger.Infof("Query result delivered succesfully to client: %s", clientID)
			}
		}
	}
}

func (g *Gateway) gracefulShutdown(ctx context.Context, listener net.Listener) {
	<-ctx.Done()
	listener.Close()

	g.runningMutex.Lock()
	g.running = false
	g.runningMutex.Unlock()

	g.clientsMutex.Lock()
	for clientID, conn := range g.clients {
		g.logger.Infof("closing connection for client %s", clientID)
		_ = conn.Close()
	}
	g.clientsMutex.Unlock()
}

func (g *Gateway) isRunning() bool {
	g.runningMutex.RLock()
	defer g.runningMutex.RUnlock()
	return g.running
}

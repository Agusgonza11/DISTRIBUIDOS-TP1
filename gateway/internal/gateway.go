package internal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/streadway/amqp"

	"github.com/op/go-logging"
	"io"
	"net"
	"strings"
	"time"
	"tp1-sistemas-distribuidos/internal/config"
)

type Gateway struct {
	config  config.Config
	running bool
	// TODO agregar running mutex
	amqpChannel *amqp.Channel
	logger      *logging.Logger
}

func NewGateway(config config.Config, logger *logging.Logger) (*Gateway, error) {
	var conn *amqp.Connection
	retries := 0
	var err error

	for retries < 3 {
		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
		if err == nil {
			break
		}

		retries++
	}

	if retries == 3 {
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
	listener, err := net.Listen("tcp", g.config.Address)
	if err != nil {
		g.logger.Errorf("failed to start gateway listener: %v", err)
		return
	}

	go func() {
		<-ctx.Done()
		listener.Close()
		g.running = false
	}()

	g.logger.Info("gateway is running and waiting for connections")

	for {
		if !g.running {
			break
		}

		g.logger.Info("waiting...")

		conn, err := listener.Accept()
		if err != nil {
			// TODO handlear
			continue
		}

		go g.handleConnection(conn)
	}
}

func (g *Gateway) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		if !g.running {
			break
		}

		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		response, err := g.readMessage(reader)
		if err != nil {
			g.logger.Errorf(fmt.Sprintf("error reading message: %v", err))
			return
		}

		lines := strings.Split(response, "\n")
		if len(lines) < 1 {
			// invalid
			continue
		}

		header := lines[0]
		splittedHeader := strings.Split(header, ",")

		messageType := strings.TrimSpace(splittedHeader[0])

		switch messageType {
		case "ARGENTINIAN-SPANISH-PRODUCTIONS":
			batchID := splittedHeader[2]
			err := g.writeMessage(conn, []byte(fmt.Sprintf("MOVIES_ACK:%s", batchID)))
			if err != nil {
				g.logger.Errorf("failed trying to send movies ack: %v", err)
				return
			}

			queueName := g.config.RabbitMQ.FilterQueues[messageType]
			var sb strings.Builder

			for _, line := range lines[1:] {
				if strings.TrimSpace(line) == "" {
					continue
				}

				elements := strings.Split(line, ",")
				if len(elements) < 8 {
					continue
				}

				id := elements[0]
				title := elements[1]
				genres := elements[5]
				productionCountries := elements[6]
				releaseDate := elements[7]

				formattedLine := fmt.Sprintf("%s,%s,%s,%s,%s\n", id, title, genres, productionCountries, releaseDate)
				sb.WriteString(formattedLine)
			}

			err = g.amqpChannel.Publish(
				"",
				queueName,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain; charset=utf-8",
					Body:        []byte(sb.String()),
				},
			)
			if err != nil {
				return
			}

		case "EOF":
			err := g.writeMessage(conn, []byte("EOF_ACK"))
			if err != nil {
				g.logger.Errorf("failed trying to eof ack: %v", err)
				return
			}
		default:
			continue
		}
	}
}

func (g *Gateway) addMessageLengthPrefix(message []byte) []byte {
	msgLength := uint16(len(message))
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, msgLength)

	return append(lengthBytes, message...)
}

func (g *Gateway) writeMessage(conn net.Conn, message []byte) error {
	message = g.addMessageLengthPrefix(message)

	writer := bufio.NewWriter(conn)

	writtenBytes := 0
	totalBytes := len(message)

	for writtenBytes < totalBytes {
		n, err := writer.Write(message[writtenBytes:])
		if err != nil {
			g.logger.Errorf("action: send_message | result: fail | error: %v", err)
			return err
		}

		writtenBytes += n
	}

	if err := writer.Flush(); err != nil {
		g.logger.Errorf("action: send_message | result: fail | error: %v", err)
		return err
	}

	return nil
}

func (g *Gateway) readMessage(reader *bufio.Reader) (string, error) {
	lengthBytes := make([]byte, 2)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return "", err
	}

	bytesRead := 0
	messageLength := binary.BigEndian.Uint16(lengthBytes)

	messageBytes := make([]byte, messageLength)

	for bytesRead < int(messageLength) {
		n, err := reader.Read(messageBytes[bytesRead:])
		if err != nil {
			return "", err
		}

		bytesRead += n
	}

	return string(messageBytes), nil
}

package input_gateway

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"tp1-sistemas-distribuidos/gateway/internal/models"
	"tp1-sistemas-distribuidos/gateway/internal/utils"

	"github.com/streadway/amqp"
)

func (g *Gateway) moviesHandler(conn net.Conn) {
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

		switch messageType {
		case models.MessageQueryArgentinaEsp:
			batchID := splittedHeader[3]
			g.logger.Infof(fmt.Sprintf("MOVIES_ACK:%s", batchID))

			err := utils.WriteMessage(conn, []byte(fmt.Sprintf("MOVIES_ACK:%s", batchID)))
			if err != nil {
				g.logger.Errorf("failed trying to send movies ack: %v", err)
				return
			}

			queueName, exists := g.config.RabbitMQ.FilterQueues[strings.ToLower(messageType)]
			if !exists {
				g.logger.Errorf("QUEUE NOT FOUND")
				return
			}

			var sb strings.Builder

			for _, line := range lines[1:] {
				if strings.TrimSpace(line) == "" {
					continue
				}

				elements := strings.Split(line, "|")
				if len(elements) < 8 {
					continue
				}

				id := elements[0]
				title := elements[1]
				genres := elements[5]
				productionCountries := elements[6]
				releaseDate := elements[7]

				formattedLine := fmt.Sprintf("%s|%s|%s|%s|%s\n", id, title, genres, productionCountries, releaseDate)
				sb.WriteString(formattedLine)
			}

			clientID := splittedHeader[2]

			body := []byte(fmt.Sprintf("%s\n%s", clientID, sb.String()))

			err = g.amqpChannel.Publish(
				"",
				queueName,
				true,
				false,
				amqp.Publishing{
					ContentType: "text/plain; charset=utf-8",
					Body:        body,
				},
			)
			if err != nil {
				g.logger.Errorf("failed trying to publish message: %v", err)
				return
			}
		case models.MessageEOF:
			g.logger.Infof("EOF_ACK sent")
			err := utils.WriteMessage(conn, []byte("EOF_ACK"))
			if err != nil {
				g.logger.Errorf("failed trying to eof ack: %v", err)
				return
			}

			return
		default:
			continue
		}
	}
}

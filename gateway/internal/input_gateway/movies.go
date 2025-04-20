package input_gateway

import (
	"bufio"
	"bytes"
	"encoding/csv"
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

			var buf bytes.Buffer
			csvWriter := csv.NewWriter(&buf)
			csvWriter.Comma = ','
			ab := []string{
				"id",
				"title",
				"genres",
				"production_countries",
				"release_date",
			}

			if err := csvWriter.Write(ab); err != nil {
				g.logger.Errorf("CSV write error: %v", err)
				continue
			}

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

				if id == "" || strings.TrimSpace(title) == "" || genres == "" || productionCountries == "" || releaseDate == "" {
					continue
				}

				record := []string{
					id,
					title,
					genres,
					productionCountries,
					releaseDate,
				}

				if err := csvWriter.Write(record); err != nil {
					g.logger.Errorf("CSV write error: %v", err)
					continue
				}
			}

			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				g.logger.Errorf("CSV flush error: %v", err)
				return
			}

			// clientID := splittedHeader[2]

			// body := []byte(fmt.Sprintf("%s\n%s", clientID, sb.String()))
			body := []byte(buf.Bytes())

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

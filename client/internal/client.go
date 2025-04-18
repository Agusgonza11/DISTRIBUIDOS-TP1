package internal

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tp1-sistemas-distribuidos/internal/config"
)

type Client struct {
	id         string
	config     config.Config
	conn       net.Conn
	outputFile *os.File
	logger     *logging.Logger
}

func NewClient(config config.Config, logger *logging.Logger) *Client {
	return &Client{
		config: config,
		logger: logger,
	}
}

func (c *Client) ProcessQuery(ctx context.Context, query string) {
	switch query {
	case QueryArgentinaEsp:
		c.processArgentinianSpanishProductions(ctx)
	case QueryTopInvestors:
		c.processTopInvestingCountries(ctx)
	case QueryTopArgentinianMoviesByRating:
		c.processTopArgentinianMoviesByRating(ctx)
	case QueryTopArgentinianActors:
		c.processTopArgentinianActors(ctx)
	case QuerySentimentAnalysis:
		c.processSentimentAnalysis(ctx)
	default:
		c.logger.Infof("unknown query type: %v", query)
	}
}

type Movie struct {
	ID                  int
	Title               string
	Overview            string
	Budget              int
	Revenue             int
	Genres              string
	ProductionCountries string
	ReleaseDate         string
}

// Movies CSV file index columns
const (
	idColumn                  = 5
	titleColumn               = 19
	overviewColumn            = 9
	budgetColumn              = 2
	revenueColumn             = 15
	genresColumn              = 3
	productionCountriesColumn = 13
	releaseDateColumn         = 14
)

func (c *Client) sendMovies() error {
	file, err := os.Open(c.config.MoviesFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	ignoreFirstCSVLine(reader)

	var currentBatch []*Movie
	var currentBatchID int
	var batchSizeBytes int

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.logger.Infof("reached end of file")
				break
			}

			c.logger.Infof("failed trying to read file: %w", err)
			break
		}

		movie := c.mapMovieFromCSVLine(line)

		movieSize, _ := json.Marshal(movie)

		if len(currentBatch) >= c.config.BatchSize || batchSizeBytes+len(movieSize) > c.config.BatchLimitAmount {
			if err := c.sendMoviesBatch(currentBatch, QueryArgentinaEsp, currentBatchID); err != nil {
				c.logger.Errorf("failed trying to send movies batch: %v", err)
				return err
			}

			currentBatch = []*Movie{}
			batchSizeBytes = 0
			currentBatchID++
		}

		currentBatch = append(currentBatch, movie)
		batchSizeBytes += len(movieSize)
	}

	if err := c.sendMoviesBatch(currentBatch, QueryArgentinaEsp, currentBatchID); err != nil {
		c.logger.Errorf("failed trying to send movies batch: %v", err)
		return err
	}

	err = c.sendEOF()
	if err != nil {
		c.logger.Errorf("failed trying to send EOF message: %v", err)
		return err
	}

	return nil
}

func (c *Client) createOutputFile(query string) error {
	path := fmt.Sprintf("/app/data/results_%s.txt", query)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		c.logger.Errorf("failed trying to create output file: %v", err)
		return err
	}

	c.outputFile = file

	return nil
}

func (c *Client) processArgentinianSpanishProductions(ctx context.Context) {
	err := c.connectToGateway(ctx)
	if err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	defer c.closeConn()

	err = c.createOutputFile(QueryArgentinaEsp)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		c.handleResults(ctx)
	}()

	err = c.sendMovies()
	if err != nil {
		return
	}

	return
}

func (c *Client) handleResults(ctx context.Context) {
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", c.config.OutputGatewayAddress)
	if err != nil {
		c.logger.Errorf("failed trying to connect to output gateway: %v", err)
		return
	}

	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	message := []byte(fmt.Sprintf("%s,%s", ClientIDMessage, c.id))

	err = c.writeMessage(c.conn, message)
	if err != nil {
		return
	}

	for {
		response, err := c.readMessage(conn)
		if err != nil {
			c.logger.Errorf(fmt.Sprintf("failed trying to fetch results: %v", err))
			return
		}

		if response == EndOfFileMessage {
			break
		}

		err = c.writeMessage(c.conn, []byte(ResultACK))
		if err != nil {
			return
		}

		c.writeResultToOutputFile(response)
	}
}

func (c *Client) sendEOF() error {
	err := c.writeMessage(c.conn, []byte(EndOfFileMessage))
	if err != nil {
		errMessage := fmt.Sprintf("error writing EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	response, err := c.readMessage(c.conn)
	if err != nil {
		errMessage := fmt.Sprintf("error reading EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	if EndOfFileACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", EndOfFileACK, response)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	return nil
}

func (c *Client) sendMoviesBatch(movies []*Movie, query string, batchID int) error {
	if len(movies) == 0 {
		return nil
	}

	message := []byte(c.buildMoviesBatchMessage(movies, query, batchID))

	err := c.writeMessage(c.conn, message)
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	response, err := c.readMessage(c.conn)
	if err != nil {
		errMessage := fmt.Sprintf("error reading batch ACK: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	expectedACK := fmt.Sprintf(MoviesACK, batchID)
	if expectedACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", expectedACK, response)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	return nil
}

func (c *Client) buildMoviesBatchMessage(movies []*Movie, query string, batchID int) string {
	var sb strings.Builder

	for _, movie := range movies {
		sb.WriteString(fmt.Sprintf("%d,%s,%s,%d,%d,%s,%s,%s\n",
			movie.ID,
			movie.Title,
			movie.Overview,
			movie.Budget,
			movie.Revenue,
			movie.Genres,
			movie.ProductionCountries,
			movie.ReleaseDate,
		))
	}

	return fmt.Sprintf("%s,%d\n%s", query, batchID, sb.String())
}

func (c *Client) addMessageLengthPrefix(message []byte) []byte {
	msgLength := uint16(len(message))
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, msgLength)

	return append(lengthBytes, message...)
}

func (c *Client) writeResultToOutputFile(result string) {
	message := []byte(result)

	writtenBytes := 0
	totalBytes := len(message)

	for writtenBytes < totalBytes {
		n, err := c.outputFile.Write(message[writtenBytes:])
		if err != nil {
			c.logger.Errorf("failed trying to write to output file: %v", err)
			return
		}

		writtenBytes += n
	}
}

func (c *Client) writeMessage(conn net.Conn, message []byte) error {
	message = c.addMessageLengthPrefix(message)

	writer := bufio.NewWriter(conn)

	writtenBytes := 0
	totalBytes := len(message)

	for writtenBytes < totalBytes {
		n, err := writer.Write(message[writtenBytes:])
		if err != nil {
			c.logger.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.id, err)
			return err
		}

		writtenBytes += n
	}

	if err := writer.Flush(); err != nil {
		c.logger.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.id, err)
		return err
	}

	return nil
}

func (c *Client) readMessage(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)

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

func ignoreFirstCSVLine(reader *csv.Reader) {
	_, err := reader.Read()
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Client) mapMovieFromCSVLine(line []string) *Movie {
	id, _ := strconv.Atoi(line[idColumn])
	budget, _ := strconv.Atoi(line[budgetColumn])
	revenue, _ := strconv.Atoi(line[revenueColumn])

	return &Movie{
		ID:                  id,
		Title:               line[titleColumn],
		Overview:            line[overviewColumn],
		Budget:              budget,
		Revenue:             revenue,
		Genres:              line[genresColumn],
		ProductionCountries: line[productionCountriesColumn],
		ReleaseDate:         line[releaseDateColumn],
	}
}

func (c *Client) processTopInvestingCountries(ctx context.Context) {
	time.Sleep(time.Hour)
}

func (c *Client) processTopArgentinianMoviesByRating(ctx context.Context) {
	time.Sleep(time.Hour)
}

func (c *Client) processTopArgentinianActors(ctx context.Context) {
	time.Sleep(time.Hour)
}

func (c *Client) processSentimentAnalysis(ctx context.Context) {
	time.Sleep(time.Hour)
}

func (c *Client) connectToGateway(ctx context.Context) error {
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", c.config.InputGatewayAddress)
	if err != nil {
		return err
	}

	c.conn = conn

	return nil
}

func (c *Client) closeConn() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

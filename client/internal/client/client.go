package client

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	goIO "io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"tp1-sistemas-distribuidos/internal/config"
	"tp1-sistemas-distribuidos/internal/models"
	io "tp1-sistemas-distribuidos/internal/utils"
)

type Client struct {
	id         string
	config     config.Config
	conns      map[string]net.Conn
	outputFile *os.File
	logger     *logging.Logger
}

func NewClient(config config.Config, logger *logging.Logger) *Client {
	return &Client{
		config: config,
		logger: logger,
		conns:  make(map[string]net.Conn),
	}
}

func (c *Client) ProcessQuery(ctx context.Context, query string) {
	switch query {
	case models.QueryArgentinaEsp:
		c.processArgentinianSpanishProductions(ctx)
	case models.QueryTopInvestors:
		c.processTopInvestingCountries(ctx)
	case models.QueryTopArgentinianMoviesByRating:
		c.processTopArgentinianMoviesByRating(ctx)
	case models.QueryTopArgentinianActors:
		c.processTopArgentinianActors(ctx)
	case models.QuerySentimentAnalysis:
		c.processSentimentAnalysis(ctx)
	default:
		c.logger.Infof("unknown query type: %v", query)
	}
}

func (c *Client) sendMovies() error {
	file, err := os.Open(c.config.MoviesFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	io.IgnoreFirstCSVLine(reader)

	var currentBatch []*models.Movie
	var currentBatchID int
	var batchSizeBytes int

	c.logger.Infof("Starting to send")

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				c.logger.Infof("reached end of file")
				break
			}

			c.logger.Infof("failed trying to read file: %v", err)
			continue
		}

		movie := c.mapMovieFromCSVLine(line)

		movieSize, _ := json.Marshal(movie)

		if len(currentBatch) >= c.config.BatchSize || batchSizeBytes+len(movieSize) > c.config.BatchLimitAmount {
			if err := c.sendMoviesBatch(currentBatch, models.QueryArgentinaEsp, currentBatchID); err != nil {
				c.logger.Errorf("failed trying to send movies batch: %v", err)
				return err
			}

			currentBatch = []*models.Movie{}
			batchSizeBytes = 0
			currentBatchID++
		}

		currentBatch = append(currentBatch, movie)
		batchSizeBytes += len(movieSize)
	}

	if err := c.sendMoviesBatch(currentBatch, models.QueryArgentinaEsp, currentBatchID); err != nil {
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

	err = c.createOutputFile(models.QueryArgentinaEsp)
	if err != nil {
		return
	}

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

	err = io.WriteMessage(c.conns["movies"], message)
	if err != nil {
		return
	}

	for {
		response, err := io.ReadMessage(conn)
		if err != nil {
			c.logger.Errorf(fmt.Sprintf("failed trying to fetch results: %v", err))
			return
		}

		if response == EndOfFileMessage {
			break
		}

		err = io.WriteMessage(c.conns["movies"], []byte(ResultACK))
		if err != nil {
			return
		}

		io.WriteFile(c.outputFile, response)
	}
}

func (c *Client) sendEOF() error {
	err := io.WriteMessage(c.conns["movies"], []byte(fmt.Sprintf("%s,%s", EndOfFileMessage, c.id)))
	if err != nil {
		errMessage := fmt.Sprintf("error writing EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	c.logger.Infof("Waiting for EOF ACK")

	response, err := io.ReadMessage(c.conns["movies"])
	if err != nil {
		errMessage := fmt.Sprintf("error reading EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	c.logger.Infof("Received EOF response: %v", response)

	if EndOfFileACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", EndOfFileACK, response)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	return nil
}

func (c *Client) sendMoviesBatch(movies []*models.Movie, query string, batchID int) error {
	if len(movies) == 0 {
		return nil
	}

	message := []byte(c.buildMoviesBatchMessage(movies, query, batchID))

	err := io.WriteMessage(c.conns["movies"], message)
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	response, err := io.ReadMessage(c.conns["movies"])
	if err != nil {
		errMessage := fmt.Sprintf("error reading batch ACK: %v", err)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	c.logger.Infof("batch response ACK: %s", response)

	expectedACK := fmt.Sprintf(MoviesACK, batchID)
	if expectedACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", expectedACK, response)
		c.logger.Errorf(errMessage)
		return fmt.Errorf(errMessage)
	}

	return nil
}

func (c *Client) buildMoviesBatchMessage(movies []*models.Movie, query string, batchID int) string {
	var sb strings.Builder

	for _, movie := range movies {
		sb.WriteString(fmt.Sprintf("%d|%s|%s|%d|%d|%s|%s|%s\n",
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

	return fmt.Sprintf("%s,movies,%s,%d\n%s", query, c.id, batchID, sb.String())
}

func (c *Client) mapMovieFromCSVLine(line []string) *models.Movie {
	id, _ := strconv.Atoi(line[models.IDColumn])
	budget, _ := strconv.Atoi(line[models.BudgetColumn])
	revenue, _ := strconv.Atoi(line[models.RevenueColumn])

	return &models.Movie{
		ID:                  id,
		Title:               line[models.TitleColumn],
		Overview:            line[models.OverviewColumn],
		Budget:              budget,
		Revenue:             revenue,
		Genres:              line[models.GenresColumn],
		ProductionCountries: line[models.ProductionCountriesColumn],
		ReleaseDate:         line[models.ReleaseDateColumn],
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
	addresses := map[string]string{
		"movies":  c.config.InputMoviesGatewayAddress,
		"credits": c.config.InputCreditsGatewayAddress,
		"ratings": c.config.InputRatingsGatewayAddress,
	}

	for service, gatewayAddress := range addresses {
		dialer := net.Dialer{}

		conn, err := dialer.DialContext(ctx, "tcp", gatewayAddress)
		if err != nil {
			return err
		}

		c.conns[service] = conn
	}

	return nil
}

func (c *Client) closeConn() {
	for _, conn := range c.conns {
		if conn != nil {
			_ = conn.Close()
			conn = nil
		}
	}
}

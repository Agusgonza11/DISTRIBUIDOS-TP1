package client

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	goIO "io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/op/go-logging"

	"tp1-sistemas-distribuidos/client/internal/config"
	"tp1-sistemas-distribuidos/client/internal/models"
	io "tp1-sistemas-distribuidos/client/internal/utils"
)

type Client struct {
	id          string
	config      config.Config
	conns       map[string]net.Conn
	outputFiles map[string]*os.File
	logger      *logging.Logger
}

func NewClient(config config.Config, logger *logging.Logger) *Client {
	clientID, err := getClientIDFromGateway(config.ConnectionsGatewayAddress, logger)
	if err != nil {
		logger.Fatalf("Failed to obtain client ID from gateway: %v", err)
	}

	return &Client{
		id:          clientID,
		config:      config,
		logger:      logger,
		conns:       make(map[string]net.Conn),
		outputFiles: make(map[string]*os.File),
	}

}

func (c *Client) ProcessQuery(ctx context.Context, queries []string) {
	defer c.closeOutputFiles()
	defer c.closeConn()

	go func() {
		c.gracefulShutdown(ctx)
	}()

	if err := c.connectToGateway(ctx); err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	wg := sync.WaitGroup{}

	var moviesQueries []string
	var sendCredits, sendRatings bool
	for _, query := range queries {
		if err := c.createOutputFile(query); err != nil {
			c.logger.Errorf("failed to create output file: %v", err)
			continue
		}

		moviesQueries = append(moviesQueries, query)

		if query == QueryTopArgentinianMoviesByRating {
			sendRatings = true
		}

		if query == QueryTopArgentinianActors {
			sendCredits = true
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = c.sendMovies(moviesQueries)
	}()

	if sendCredits {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.sendCredits(QueryTopArgentinianActors)
		}()
	}

	if sendRatings {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.sendRatings(QueryTopArgentinianMoviesByRating)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, len(queries))
	}()

	wg.Wait()
}

func (c *Client) sendMovies(queries []string) error {
	file, err := os.Open(c.config.MoviesFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	io.IgnoreFirstCSVLine(reader)

	var batch []*models.Movie
	var batchID int
	var batchSize int

	c.logger.Infof("Starting to send Movies for queries %s", strings.Join(queries, ","))

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			continue
		}

		movie := c.mapMovieFromCSVLine(line)

		movieSize, _ := json.Marshal(movie)

		if len(batch) >= c.config.BatchSize || batchSize+len(movieSize) > c.config.BatchLimitAmount {
			if err := c.sendMoviesBatch(batch, queries, batchID); err != nil {
				c.logger.Errorf("failed trying to send movies batch: %v", err)
				return err
			}

			batch = []*models.Movie{}
			batchSize = 0
			batchID++
		}

		batch = append(batch, movie)
		batchSize += len(movieSize)
	}

	if err := c.sendMoviesBatch(batch, queries, batchID); err != nil {
		c.logger.Errorf("failed trying to send movies batch: %v", err)
		return err
	}

	err = c.sendEOF(strings.Join(queries, "|"), MoviesService)
	if err != nil {
		c.logger.Errorf("failed trying to send EOF message: %v", err)
		return err
	}

	return nil
}

func (c *Client) sendCredits(query string) error {
	file, err := os.Open(c.config.CreditsFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	io.IgnoreFirstCSVLine(reader)

	var batch []*models.Credit
	var batchID int
	var batchSize int

	c.logger.Infof("Starting to send Credits for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			continue
		}

		credit := c.mapCreditFromCSVLine(line)

		creditSize, _ := json.Marshal(credit)

		if len(batch) >= c.config.BatchSize || batchSize+len(creditSize) > c.config.BatchLimitAmount {
			if err := c.sendCreditsBatch(batch, query, batchID); err != nil {
				c.logger.Errorf("failed trying to send credits batch: %v", err)
				return err
			}

			batch = []*models.Credit{}
			batchSize = 0
			batchID++
		}

		batch = append(batch, credit)
		batchSize += len(creditSize)
	}

	if err := c.sendCreditsBatch(batch, query, batchID); err != nil {
		c.logger.Errorf("failed trying to send credits batch: %v", err)
		return err
	}

	err = c.sendEOF(query, CreditsService)
	if err != nil {
		c.logger.Errorf("failed trying to send EOF message: %v", err)
		return err
	}

	return nil
}

func (c *Client) sendRatings(query string) error {
	file, err := os.Open(c.config.RatingsFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	io.IgnoreFirstCSVLine(reader)

	var batch []*models.Rating
	var batchID int
	var batchSize int

	c.logger.Infof("Starting to send Ratings for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			continue
		}

		rating := c.mapRatingFromCSVLine(line)

		ratingSize, _ := json.Marshal(rating)

		if len(batch) >= c.config.BatchSize || batchSize+len(ratingSize) > c.config.BatchLimitAmount {
			if err := c.sendRatingsBatch(batch, query, batchID); err != nil {
				c.logger.Errorf("failed trying to send ratings batch: %v", err)
				return err
			}

			batch = []*models.Rating{}
			batchSize = 0
			batchID++
		}

		batch = append(batch, rating)
		batchSize += len(ratingSize)
	}

	if err := c.sendRatingsBatch(batch, query, batchID); err != nil {
		c.logger.Errorf("failed trying to send ratings batch: %v", err)
		return err
	}

	err = c.sendEOF(query, RatingsService)
	if err != nil {
		c.logger.Errorf("failed trying to send EOF message: %v", err)
		return err
	}

	return nil
}

func (c *Client) createOutputFile(query string) error {
	path := fmt.Sprintf("/app/data/%s_results_%s.txt", c.id, query)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		c.logger.Errorf("failed trying to create output file: %v", err)
		return err
	}

	c.outputFiles[query] = file

	return nil
}

func (c *Client) handleResults(ctx context.Context, totalQueries int) {
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

	c.logger.Infof("Connected to output gateway at address: %s", c.config.OutputGatewayAddress)

	message := []byte(fmt.Sprintf("%s,%s", ClientIDMessage, c.id))

	err = io.WriteMessage(conn, message)
	if err != nil {
		return
	}

	c.logger.Info("Handling message results")

	for {
		if totalQueries == 0 {
			break
		}

		response, err := io.ReadMessage(conn)
		if err != nil {
			c.logger.Errorf(fmt.Sprintf("failed trying to fetch results: %v", err))
			return
		}

		c.logger.Infof("Received message %s", response)

		lines := strings.Split(response, "\n")
		if len(lines) < 1 {
			continue
		}

		query := lines[0]

		if strings.TrimSpace(lines[1]) == EndOfFileMessage {
			totalQueries--
			c.logger.Infof("Query %s received successfully!", query)
			continue
		}

		err = io.WriteMessage(conn, []byte(ResultACK))
		if err != nil {
			return
		}

		io.WriteFile(c.outputFiles[query], strings.Join(lines[1:], "\n"))
	}
}

func (c *Client) sendEOF(query string, service string) error {
	err := io.WriteMessage(c.conns[service], []byte(fmt.Sprintf("%s,%s,%s,%s", query, service, c.id, EndOfFileMessage)))
	if err != nil {
		errMessage := fmt.Sprintf("error writing EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	c.logger.Infof("Waiting for EOF ACK")

	response, err := io.ReadMessage(c.conns[service])
	if err != nil {
		errMessage := fmt.Sprintf("error reading EOF message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	c.logger.Infof("Received EOF response: %v", response)

	if eofACKs[service] != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", EndOfFileACK, response)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	return nil
}

func (c *Client) sendMoviesBatch(batch []*models.Movie, queries []string, batchID int) error {
	if len(batch) == 0 {
		return nil
	}

	message := c.buildMoviesBatchMessage(batch, queries, batchID)

	err := io.WriteMessage(c.conns[MoviesService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[MoviesService])
	if err != nil {
		errMessage := fmt.Sprintf("error reading batch ACK: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	expectedACK := fmt.Sprintf(MoviesACK, batchID)
	if expectedACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", expectedACK, response)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	return nil
}

func (c *Client) sendCreditsBatch(batch []*models.Credit, query string, batchID int) error {
	if len(batch) == 0 {
		return nil
	}

	message := c.buildCreditsBatchMessage(batch, query, batchID)

	err := io.WriteMessage(c.conns[CreditsService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[CreditsService])
	if err != nil {
		errMessage := fmt.Sprintf("error reading batch ACK: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	expectedACK := fmt.Sprintf(CreditsACK, batchID)
	if expectedACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", expectedACK, response)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	return nil
}

func (c *Client) sendRatingsBatch(batch []*models.Rating, query string, batchID int) error {
	if len(batch) == 0 {
		return nil
	}

	message := c.buildRatingsBatchMessage(batch, query, batchID)

	err := io.WriteMessage(c.conns[RatingsService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[RatingsService])
	if err != nil {
		errMessage := fmt.Sprintf("error reading batch ACK: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	expectedACK := fmt.Sprintf(RatingsACK, batchID)
	if expectedACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", expectedACK, response)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	return nil
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

func (c *Client) mapRatingFromCSVLine(line []string) *models.Rating {
	movieId, _ := strconv.Atoi(line[models.RatingsMovieIDColumn])
	rating, _ := strconv.ParseFloat(line[models.RatingColumn], 64)

	return &models.Rating{
		ID:     movieId,
		Rating: rating,
	}
}

func (c *Client) mapCreditFromCSVLine(line []string) *models.Credit {
	id, _ := strconv.Atoi(line[models.CreditsMovieIDColumn])

	return &models.Credit{
		ID:   id,
		Cast: line[models.CastColumn],
	}
}

func (c *Client) connectToGateway(ctx context.Context) error {
	addresses := map[string]string{
		MoviesService:  c.config.InputMoviesGatewayAddress,
		CreditsService: c.config.InputCreditsGatewayAddress,
		RatingsService: c.config.InputRatingsGatewayAddress,
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

func (c *Client) buildMoviesBatchMessage(movies []*models.Movie, queries []string, batchID int) string {
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

	queryString := strings.Join(queries, "|")
	return fmt.Sprintf("%s,MOVIES,%s,%d\n%s", queryString, c.id, batchID, sb.String())

}

func (c *Client) buildCreditsBatchMessage(credits []*models.Credit, query string, batchID int) string {
	var sb strings.Builder

	for _, credit := range credits {
		sb.WriteString(fmt.Sprintf("%d|%s\n", credit.ID, credit.Cast))
	}

	return fmt.Sprintf("%s,CREDITS,%s,%d\n%s", query, c.id, batchID, sb.String())
}

func (c *Client) buildRatingsBatchMessage(ratings []*models.Rating, query string, batchID int) string {
	var sb strings.Builder

	for _, rating := range ratings {
		sb.WriteString(fmt.Sprintf("%d|%f\n", rating.ID, rating.Rating))
	}

	return fmt.Sprintf("%s,RATINGS,%s,%d\n%s", query, c.id, batchID, sb.String())
}

func (c *Client) gracefulShutdown(ctx context.Context) {
	<-ctx.Done()
	c.closeOutputFiles()
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *Client) closeOutputFiles() {
	for _, file := range c.outputFiles {
		if file != nil {
			file.Close()
		}
	}
}

func getClientIDFromGateway(connectionAddr string, logger *logging.Logger) (string, error) {
	const maxRetries = 3
	const retryInterval = 5 * time.Second

	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err := net.Dial("tcp", connectionAddr)
		if err != nil {
			logger.Warningf("attempt %d: could not connect to gateway in %s: %v", attempt, connectionAddr, err)
			lastErr = err
			if attempt < maxRetries {
				time.Sleep(retryInterval)
			}
			continue
		}

		defer conn.Close()

		id, err := io.ReadMessage(conn)
		if err != nil {
			return "", fmt.Errorf("could not read client-id from gateway: %w", err)
		}

		logger.Infof("assigned client-id from gateway: %s", id)
		return strings.TrimSpace(id), nil
	}

	return "", fmt.Errorf("could not connect to gateway in %s after %d retries: %w", connectionAddr, maxRetries, lastErr)
}

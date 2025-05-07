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
	go func() {
		c.gracefulShutdown(ctx)
	}()
	for _, query := range queries {
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
		c.sendMovies(QueryArgentinaEsp)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, QueryArgentinaEsp)
	}()

	wg.Wait()
}

func (c *Client) processTopInvestingCountries(ctx context.Context) {
	err := c.connectToGateway(ctx)
	if err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	defer c.closeConn()

	err = c.createOutputFile(QueryTopInvestors)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, QueryTopInvestors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(QueryTopInvestors)
	}()

	wg.Wait()
}

func (c *Client) processTopArgentinianMoviesByRating(ctx context.Context) {
	err := c.connectToGateway(ctx)
	if err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	defer c.closeConn()

	err = c.createOutputFile(QueryTopArgentinianMoviesByRating)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, QueryTopArgentinianMoviesByRating)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(QueryTopArgentinianMoviesByRating)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendRatings(QueryTopArgentinianMoviesByRating)
	}()

	wg.Wait()
}

func (c *Client) processTopArgentinianActors(ctx context.Context) {
	err := c.connectToGateway(ctx)
	if err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	defer c.closeConn()

	err = c.createOutputFile(QueryTopArgentinianActors)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, QueryTopArgentinianActors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(QueryTopArgentinianActors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendCredits(QueryTopArgentinianActors)
	}()

	wg.Wait()
}

func (c *Client) processSentimentAnalysis(ctx context.Context) {
	err := c.connectToGateway(ctx)
	if err != nil {
		c.logger.Errorf("failed trying to connect to input gateway: %v", err)
		return
	}

	defer c.closeConn()

	err = c.createOutputFile(QuerySentimentAnalysis)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, QuerySentimentAnalysis)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(QuerySentimentAnalysis)
	}()

	wg.Wait()
}

func (c *Client) sendMovies(query string) error {
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

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			c.logger.Infof("failed trying to read file: %v", err)
			continue
		}

		movie := c.mapMovieFromCSVLine(line)

		movieSize, _ := json.Marshal(movie)

		if len(batch) >= c.config.BatchSize || batchSize+len(movieSize) > c.config.BatchLimitAmount {
			if err := c.sendMoviesBatch(batch, query, batchID); err != nil {
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

	if err := c.sendMoviesBatch(batch, query, batchID); err != nil {
		c.logger.Errorf("failed trying to send movies batch: %v", err)
		return err
	}

	err = c.sendEOF(query, MoviesService)
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

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			c.logger.Infof("failed trying to read file: %v", err)
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

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				break
			}

			c.logger.Infof("failed trying to read file: %v", err)
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

func (c *Client) handleResults(ctx context.Context, query string) {
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

	for {
		c.logger.Infof("Handling message result for query %s", query)
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
			c.logger.Infof("Query %s received successfully!", query)
			break
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

	if EndOfFileACK != strings.TrimSpace(response) {
		errMessage := fmt.Sprintf("expected message ACK '%s', got '%s'", EndOfFileACK, response)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	return nil
}

func (c *Client) sendMoviesBatch(batch []*models.Movie, query string, batchID int) error {
	if len(batch) == 0 {
		return nil
	}

	message := c.buildMoviesBatchMessage(batch, query, batchID)

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

	return fmt.Sprintf("%s,MOVIES,%s,%d\n%s", query, c.id, batchID, sb.String())
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
	for _, conn := range c.conns {
		conn.Close()
	}
}

func getClientIDFromGateway(connectionAddr string, logger *logging.Logger) (string, error) {
	conn, err := net.Dial("tcp", connectionAddr)
	if err != nil {
		return "", fmt.Errorf("could not connect to gateway in %s: %w", connectionAddr, err)
	}

	defer conn.Close()

	id, err := io.ReadMessage(conn)
	if err != nil {
		return "", fmt.Errorf("could not read client-id from gateway: %w", err)
	}

	logger.Infof("assigned client-id from gateway: %s", id)

	return strings.TrimSpace(id), nil
}

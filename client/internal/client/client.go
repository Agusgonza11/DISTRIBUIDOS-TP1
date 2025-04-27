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
	"tp1-sistemas-distribuidos/client/internal/config"
	"tp1-sistemas-distribuidos/client/internal/models"
	io "tp1-sistemas-distribuidos/client/internal/utils"

	"github.com/op/go-logging"
)

type Client struct {
	id          string
	config      config.Config
	conns       map[string]net.Conn
	outputFiles map[string]*os.File
	logger      *logging.Logger
}

func NewClient(config config.Config, logger *logging.Logger) *Client {
	return &Client{
		id:          "TESTING",
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
}

func (c *Client) sendMovies(query string) error {
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

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				// c.logger.Infof("reached end of file")
				break
			}

			// c.logger.Infof("failed trying to read file: %v", err)
			continue
		}

		movie := c.mapMovieFromCSVLine(line)

		movieSize, _ := json.Marshal(movie)

		if len(currentBatch) >= c.config.BatchSize || batchSizeBytes+len(movieSize) > c.config.BatchLimitAmount {
			if err := c.sendMoviesBatch(currentBatch, query, currentBatchID); err != nil {
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

	if err := c.sendMoviesBatch(currentBatch, query, currentBatchID); err != nil {
		c.logger.Errorf("failed trying to send movies batch: %v", err)
		return err
	}

	err = c.sendEOF(query, models.MoviesService)
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

	var currentBatch []*models.Credit
	var currentBatchID int
	var batchSizeBytes int

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				// c.logger.Infof("reached end of file")
				break
			}

			// c.logger.Infof("failed trying to read file: %v", err)
			continue
		}

		credit := c.mapCreditFromCSVLine(line)

		creditSize, _ := json.Marshal(credit)

		if len(currentBatch) >= c.config.BatchSize || batchSizeBytes+len(creditSize) > c.config.BatchLimitAmount {
			if err := c.sendCreditsBatch(currentBatch, query, currentBatchID); err != nil {
				c.logger.Errorf("failed trying to send credits batch: %v", err)
				return err
			}

			currentBatch = []*models.Credit{}
			batchSizeBytes = 0
			currentBatchID++
		}

		currentBatch = append(currentBatch, credit)
		batchSizeBytes += len(creditSize)
	}

	if err := c.sendCreditsBatch(currentBatch, query, currentBatchID); err != nil {
		c.logger.Errorf("failed trying to send credits batch: %v", err)
		return err
	}

	err = c.sendEOF(query, models.CreditsService)
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

	var currentBatch []*models.Rating
	var currentBatchID int
	var batchSizeBytes int

	c.logger.Infof("Starting to send for query %s", query)

	for {
		line, err := reader.Read()
		if err != nil {
			if errors.Is(err, goIO.EOF) {
				// c.logger.Infof("reached end of file")
				break
			}

			c.logger.Infof("failed trying to read file: %v", err)
			continue
		}

		rating := c.mapRatingFromCSVLine(line)

		ratingSize, _ := json.Marshal(rating)

		if len(currentBatch) >= c.config.BatchSize || batchSizeBytes+len(ratingSize) > c.config.BatchLimitAmount {
			if err := c.sendRatingsBatch(currentBatch, query, currentBatchID); err != nil {
				c.logger.Errorf("failed trying to send ratings batch: %v", err)
				return err
			}

			currentBatch = []*models.Rating{}
			batchSizeBytes = 0
			currentBatchID++
		}

		currentBatch = append(currentBatch, rating)
		batchSizeBytes += len(ratingSize)
	}

	if err := c.sendRatingsBatch(currentBatch, query, currentBatchID); err != nil {
		c.logger.Errorf("failed trying to send ratings batch: %v", err)
		return err
	}

	err = c.sendEOF(query, models.RatingsService)
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

	c.outputFiles[query] = file

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

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(models.QueryArgentinaEsp)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, models.QueryArgentinaEsp)
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

	err = c.createOutputFile(models.QueryTopInvestors)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, models.QueryTopInvestors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(models.QueryTopInvestors)
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

	err = c.createOutputFile(models.QueryTopArgentinianMoviesByRating)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, models.QueryTopArgentinianMoviesByRating)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(models.QueryTopArgentinianMoviesByRating)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendRatings(models.QueryTopArgentinianMoviesByRating)
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

	err = c.createOutputFile(models.QueryTopArgentinianActors)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, models.QueryTopArgentinianActors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(models.QueryTopArgentinianActors)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendCredits(models.QueryTopArgentinianActors)
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

	err = c.createOutputFile(models.QuerySentimentAnalysis)
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.handleResults(ctx, models.QuerySentimentAnalysis)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.sendMovies(models.QuerySentimentAnalysis)
	}()

	wg.Wait()
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

	err := io.WriteMessage(c.conns[models.MoviesService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[models.MoviesService])
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

	err := io.WriteMessage(c.conns[models.CreditsService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[models.CreditsService])
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

	err := io.WriteMessage(c.conns[models.RatingsService], []byte(message))
	if err != nil {
		errMessage := fmt.Sprintf("error writing batch message: %v", err)
		c.logger.Errorf(errMessage)
		return errors.New(errMessage)
	}

	response, err := io.ReadMessage(c.conns[models.RatingsService])
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
		models.MoviesService:  c.config.InputMoviesGatewayAddress,
		models.CreditsService: c.config.InputCreditsGatewayAddress,
		models.RatingsService: c.config.InputRatingsGatewayAddress,
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

func convertToInterfaceSlice[T any](input []*T) []interface{} {
	result := make([]interface{}, len(input))
	for i, v := range input {
		result[i] = v
	}
	return result
}

//func sendData[T any](
//	filePath string,
//	mapFunc func([]string) *T,
//	sendBatchFunc func([]interface{}, string, string, int) error,
//	eofFunc func(string) error,
//	service string,
//	query string,
//	batchSize int,
//	batchLimitAmount int,
//	logger *logging.Logger) error {
//	file, err := os.Open(filePath)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	defer file.Close()
//
//	reader := csv.NewReader(file)
//	io.IgnoreFirstCSVLine(reader)
//
//	var currentBatch []*T
//	var currentBatchID int
//	var batchSizeBytes int
//
//	logger.Infof("Starting to send %s", service)
//
//	for {
//		line, err := reader.Read()
//		if err != nil {
//			if errors.Is(err, goIO.EOF) {
//				logger.Infof("Reached end of file")
//				break
//			}
//
//			logger.Infof("Failed trying to read file: %v", err)
//			continue
//		}
//
//		item := mapFunc(line)
//
//		itemSize, _ := json.Marshal(item)
//
//		if len(currentBatch) >= batchSize || batchSizeBytes+len(itemSize) > batchLimitAmount {
//			if err := sendBatchFunc(convertToInterfaceSlice(currentBatch), service, query, currentBatchID); err != nil {
//				logger.Errorf("Failed trying to send %s batch: %v", service, err)
//				return err
//			}
//
//			currentBatch = []*T{}
//			batchSizeBytes = 0
//			currentBatchID++
//		}
//
//		currentBatch = append(currentBatch, item)
//		batchSizeBytes += len(itemSize)
//	}
//
//	if err := sendBatchFunc(convertToInterfaceSlice(currentBatch), service, query, currentBatchID); err != nil {
//		logger.Errorf("Failed trying to send %s batch: %v", service, err)
//		return err
//	}
//
//	err = eofFunc(service)
//	if err != nil {
//		logger.Errorf("Failed trying to send EOF message: %v", err)
//		return err
//	}
//
//	return nil
//}
//var batchMappingByService = map[string]func(interface{}, string, string, int) string{
//	models.MoviesService:  buildMoviesBatchMessage,
//	models.CreditsService: buildCreditsBatchMessage,
//	models.RatingsService: buildRatingsBatchMessage,
//}

//func (c *Client) sendMovies(query string) error {
//return sendData(
//c.config.MoviesFilePath,
//c.mapMovieFromCSVLine,
//c.sendBatch,
//c.sendEOF,
//models.MoviesService,
//query,
//c.config.BatchSize,
//c.config.BatchLimitAmount,
//c.logger,
//)
//}

//	func (c *Client) sendRatings(query string) error {
//		return sendData(
//			c.config.RatingsFilePath,
//			c.mapRatingFromCSVLine,
//			c.sendBatch,
//			c.sendEOF,
//			models.RatingsService,
//			query,
//			c.config.BatchSize,
//			c.config.BatchLimitAmount,
//			c.logger,
//		)
//	}
//
//	func (c *Client) sendCredits(query string) error {
//		return sendData(
//			c.config.CreditsFilePath,
//			c.mapCreditFromCSVLine,
//			c.sendBatch,
//			c.sendEOF,
//			models.CreditsService,
//			query,
//			c.config.BatchSize,
//			c.config.BatchLimitAmount,
//			c.logger,
//		)
//	}

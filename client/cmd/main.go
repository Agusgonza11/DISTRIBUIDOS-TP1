package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"tp1-sistemas-distribuidos/client/internal/client"
	"tp1-sistemas-distribuidos/client/internal/config"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const defaultBatchLimitAmount = 8192

func main() {
	var log = logging.MustGetLogger("main")

	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(log, v)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	batchLimitAmount := v.GetInt("batch.max_amount")
	if batchLimitAmount == 0 {
		batchLimitAmount = defaultBatchLimitAmount
	}

	config := config.Config{
		InputMoviesGatewayAddress:  v.GetString("gateway.input_movies_address"),
		InputCreditsGatewayAddress: v.GetString("gateway.input_credits_address"),
		InputRatingsGatewayAddress: v.GetString("gateway.input_ratings_address"),
		OutputGatewayAddress:       v.GetString("gateway.output_address"),
		MoviesFilePath:             v.GetString("file.movies_path"),
		RatingsFilePath:            v.GetString("file.ratings_path"),
		CreditsFilePath:            v.GetString("file.credits_path"),
		BatchSize:                  v.GetInt("batch.max_size"),
		BatchLimitAmount:           batchLimitAmount,
	}

	query := v.GetString("query")

	client := client.NewClient(config, logging.MustGetLogger("client"))

	client.ProcessQuery(ctx, query)
}

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("query")
	v.BindEnv("gateway.input_movies_address")
	v.BindEnv("gateway.input_credits_address")
	v.BindEnv("gateway.input_ratings_address")
	v.BindEnv("gateway", "output_address")
	v.BindEnv("file", "movies_path")
	v.BindEnv("file", "ratings_path")
	v.BindEnv("file", "credits_path")
	v.BindEnv("batch", "max_size")
	v.BindEnv("batch", "max_amount")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./client_config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(logger *logging.Logger, v *viper.Viper) {
	logger.Infof(
		"action: config | result: success | input_movies_address: %s | input_credits_address: %s | input_ratings_address: %s | output_server_address: %s | log_level: %s | "+
			"movies_file_path: %s | ratings_file_path: %s | credits_file_path: %s | query: %s | batch_max_size: %d | batch_max_amount: %d",
		v.GetString("gateway.input_movies_address"),
		v.GetString("gateway.input_credits_address"),
		v.GetString("gateway.input_ratings_address"),
		v.GetString("gateway.output_address"),
		v.GetString("log.level"),
		v.GetString("file.movies_path"),
		v.GetString("file.ratings_path"),
		v.GetString("file.credits_path"),
		v.GetString("query"),
		v.GetInt("batch.max_size"),
		v.GetInt("batch.max_amount"),
	)
}

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"tp1-sistemas-distribuidos/gateway/internal/config"
	"tp1-sistemas-distribuidos/gateway/internal/message_broker"
	"tp1-sistemas-distribuidos/gateway/internal/output_gateway"
)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	config := config.OutputGatewayConfig{
		Address: v.GetString("gateway.address"),
		RabbitMQ: config.RabbitMQConfig{
			Address:         v.GetString("rabbitmq.address"),
			OutputQueueName: v.GetString("rabbitmq.output_queue_name"),
		},
	}

	broker, err := message_broker.NewBroker(config.RabbitMQ, logging.MustGetLogger("message_broker"))
	if err != nil {
		log.Criticalf("Error starting message broker: %s", err)
		os.Exit(1)
	}

	gateway, err := output_gateway.NewGateway(broker, config, logging.MustGetLogger("gateway"))
	if err != nil {
		log.Criticalf("Error starting gateway: %s", err)
		os.Exit(1)
	}

	gateway.Start(ctx)
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
	v.BindEnv("gateway.address")
	v.BindEnv("log", "level")
	v.BindEnv("rabbitmq.address")
	v.BindEnv("rabbitmq.output_queue_name")
	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./output_gateway_config.yaml")
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
		"action: config | result: success | movies_address: %s | credits_address: %s | ratings_address: %s | log_level: %s | rabbitmq address: %s",
		v.GetString("gateway.movies_address"),
		v.GetString("gateway.credits_address"),
		v.GetString("gateway.ratings_address"),
		v.GetString("log.level"),
		v.GetString("rabbitmq.address"),
	)

	filterQueues := v.GetStringMapString("rabbitmq.filter_queues")
	for key, queue := range filterQueues {
		logger.Infof("filter queue: %s | queue name: %s", key, queue)
	}
}

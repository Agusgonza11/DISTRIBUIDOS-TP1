package config

type InputGatewayConfig struct {
	ConnectionsAddress string
	MoviesAddress      string
	CreditsAddress     string
	RatingsAddress     string
	RabbitMQ           RabbitMQConfig
	EOFsCount          map[string]int
}

type OutputGatewayConfig struct {
	Address  string
	RabbitMQ RabbitMQConfig
}

type RabbitMQConfig struct {
	Address         string
	FilterQueues    map[string]string
	JoinQueues      map[string]string
	OutputQueueName string
}

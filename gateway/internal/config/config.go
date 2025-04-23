package config

type InputGatewayConfig struct {
	MoviesAddress  string
	CreditsAddress string
	RatingsAddress string
	RabbitMQ       RabbitMQConfig
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

package config

type Config struct {
	MoviesAddress  string
	CreditsAddress string
	RatingsAddress string
	RabbitMQ       RabbitMQConfig
}

type RabbitMQConfig struct {
	Address      string
	FilterQueues map[string]string
}

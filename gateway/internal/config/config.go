package config

type Config struct {
	Address  string
	RabbitMQ RabbitMQConfig
}

type RabbitMQConfig struct {
	Address      string
	FilterQueues map[string]string
}

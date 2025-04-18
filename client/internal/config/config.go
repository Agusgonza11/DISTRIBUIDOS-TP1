package config

type Config struct {
	InputGatewayAddress  string
	OutputGatewayAddress string
	MoviesFilePath       string
	RatingsFilePath      string
	CreditsFilePath      string
	BatchSize            int
	BatchLimitAmount     int
}

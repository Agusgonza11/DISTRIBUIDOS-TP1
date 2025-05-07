package config

type Config struct {
	ConnectionsGatewayAddress  string
	InputMoviesGatewayAddress  string
	InputCreditsGatewayAddress string
	InputRatingsGatewayAddress string
	OutputGatewayAddress       string
	MoviesFilePath             string
	RatingsFilePath            string
	CreditsFilePath            string
	BatchSize                  int
	BatchLimitAmount           int
}

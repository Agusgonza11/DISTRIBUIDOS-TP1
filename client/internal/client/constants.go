package client

const ClientIDMessage = "CLIENT_ID"
const EndOfFileMessage = "EOF"

const MoviesACK = "MOVIES_ACK:%d"
const CreditsACK = "CREDITS_ACK:%d"
const RatingsACK = "RATINGS_ACK:%d"
const ResultACK = "RESULT_ACK"
const EndOfFileACK = "EOF_ACK"

const (
	QueryArgentinaEsp                 string = "ARGENTINIAN-SPANISH-PRODUCTIONS"  // Consulta 1
	QueryTopInvestors                 string = "TOP-INVESTING-COUNTRIES"          // Consulta 2
	QueryTopArgentinianMoviesByRating string = "TOP-ARGENTINIAN-MOVIES-BY-RATING" // Consulta 3
	QueryTopArgentinianActors         string = "TOP-ARGENTINIAN-ACTORS"           // Consulta 4
	QuerySentimentAnalysis                   = "SENTIMENT-ANALYSIS"               // Consulta 5
)

const (
	MoviesService  string = "MOVIES"
	CreditsService string = "CREDITS"
	RatingsService string = "RATINGS"
)

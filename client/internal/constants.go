package internal

const (
	QueryArgentinaEsp                 string = "argentinian-spanish-productions"  // Consulta 1
	QueryTopInvestors                 string = "top-investing-countries"          // Consulta 2
	QueryTopArgentinianMoviesByRating string = "top-argentinian-movies-by-rating" // Consulta 3
	QueryTopArgentinianActors         string = "top-argentinian-actors"           // Consulta 4
	QuerySentimentAnalysis            string = "sentiment-analysis"               // Consulta 5
)

const ClientIDMessage = "CLIENT_ID"
const EndOfFileMessage = "EOF"

const MoviesACK = "MOVIES_ACK:%d"
const ResultACK = "RESULT_ACK"
const EndOfFileACK = "EOF_ACK"

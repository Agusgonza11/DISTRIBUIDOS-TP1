package models

type Rating struct {
	ID     int
	Rating float64
}

// Ratings CSV file index columns
const (
	RatingsMovieIDColumn = 1
	RatingColumn         = 2
)

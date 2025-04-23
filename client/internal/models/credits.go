package models

type Credit struct {
	ID   int
	Cast string
}

// Credits CSV file index columns
const (
	CreditsMovieIDColumn = 2
	CastColumn           = 0
)

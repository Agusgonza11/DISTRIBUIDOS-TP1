package models

type Movie struct {
	ID                  int
	Title               string
	Overview            string
	Budget              int
	Revenue             int
	Genres              string
	ProductionCountries string
	ReleaseDate         string
}

// Movies CSV file index columns
const (
	IDColumn                  = 5
	TitleColumn               = 20
	OverviewColumn            = 9
	BudgetColumn              = 2
	RevenueColumn             = 15
	GenresColumn              = 3
	ProductionCountriesColumn = 13
	ReleaseDateColumn         = 14
)

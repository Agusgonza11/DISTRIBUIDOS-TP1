package input_gateway

import (
	"bytes"
	"encoding/csv"
	"errors"
	"strings"
)

func (g *Gateway) buildMoviesMessage(lines []string, query string) ([]byte, error) {
	var buf bytes.Buffer

	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = ','

	if err := csvWriter.Write(g.getMovieColumnsByQuery(query)); err != nil {
		return nil, err
	}

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		elements := strings.Split(line, "|")
		if len(elements) < 8 {
			continue
		}

		record, err := g.getMessageElementsByQuery(elements, query)
		if err != nil {
			continue
		}

		if err := csvWriter.Write(record); err != nil {
			continue
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g *Gateway) getMessageElementsByQuery(elements []string, query string) ([]string, error) {
	switch query {
	case QueryArgentinaEsp, QueryTopArgentinianMoviesByRating, QueryTopArgentinianActors:
		id := elements[0]
		title := elements[1]
		genres := elements[5]
		productionCountries := elements[6]
		releaseDate := elements[7]

		if id == "" || strings.TrimSpace(title) == "" || genres == "" || productionCountries == "" || releaseDate == "" {
			break
		}

		return []string{
			id,
			title,
			genres,
			productionCountries,
			releaseDate,
		}, nil
	case QuerySentimentAnalysis:
		overview := elements[2]
		budget := elements[3]
		revenue := elements[4]

		if overview == "" || budget == "" || revenue == "" {
			break
		}

		return []string{
			overview,
			budget,
			revenue,
		}, nil
	case QueryTopInvestors:
		productionCountries := elements[6]
		budget := elements[3]

		if productionCountries == "" || budget == "" {
			break
		}

		return []string{
			productionCountries,
			budget,
		}, nil
	}

	return []string{}, errors.New("invalid message")
}

func (g *Gateway) getMovieColumnsByQuery(query string) []string {
	switch query {
	case QueryArgentinaEsp, QueryTopArgentinianMoviesByRating, QueryTopArgentinianActors:
		return []string{
			"id",
			"title",
			"genres",
			"production_countries",
			"release_date",
		}
	case QuerySentimentAnalysis:
		return []string{
			"overview",
			"budget",
			"revenue",
		}
	case QueryTopInvestors:
		return []string{
			"production_countries",
			"budget",
		}
	default:
		return []string{}
	}
}

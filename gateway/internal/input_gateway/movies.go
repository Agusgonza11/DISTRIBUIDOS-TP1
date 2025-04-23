package input_gateway

import (
	"bytes"
	"encoding/csv"
	"strings"
)

func (g *Gateway) buildMoviesMessage(lines []string) ([]byte, error) {
	var buf bytes.Buffer

	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = ','

	columns := []string{
		"id",
		"title",
		"genres",
		"production_countries",
		"release_date",
	}

	if err := csvWriter.Write(columns); err != nil {
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

		id := elements[0]
		title := elements[1]
		genres := elements[5]
		productionCountries := elements[6]
		releaseDate := elements[7]

		if id == "" || strings.TrimSpace(title) == "" || genres == "" || productionCountries == "" || releaseDate == "" {
			continue
		}

		record := []string{
			id,
			title,
			genres,
			productionCountries,
			releaseDate,
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

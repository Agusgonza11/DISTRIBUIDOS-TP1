package input_gateway

import (
	"bytes"
	"encoding/csv"
	"strings"
)

func (g *Gateway) buildRatingsMessage(lines []string, _ string) ([]byte, error) {
	var buf bytes.Buffer

	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = ','

	columns := []string{
		"id",
		"rating",
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
		rating := elements[1]

		if id == "" || rating == "" {
			continue
		}

		record := []string{
			id,
			rating,
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

package input_gateway

import (
	"bytes"
	"encoding/csv"
	"strings"
)

func (g *Gateway) buildCreditsMessage(lines []string, _ string) ([]byte, error) {
	var buf bytes.Buffer

	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = ','

	columns := []string{
		"id",
		"cast",
	}

	if err := csvWriter.Write(columns); err != nil {
		return nil, err
	}

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		elements := strings.Split(line, "|")
		if len(elements) < 2 {
			continue
		}

		id := elements[0]
		cast := elements[1]

		if id == "" || strings.TrimSpace(cast) == "" {
			continue
		}

		record := []string{
			id,
			cast,
		}
		//g.logger.Infof("Lo que voy a enviar es %s", record)
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

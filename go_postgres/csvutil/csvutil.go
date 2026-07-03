// Package csvutil provides strict CSV parsing helpers for TSE election datasets.
package csvutil

import (
	"encoding/csv"
	"io"
	"strings"

	"golang.org/x/text/encoding/charmap"
)

// NewLatin1Reader returns a CSV reader for TSE files (ISO-8859-1, semicolon-separated).
// LazyQuotes is disabled so embedded nicknames like "CATRAQUINHA" are parsed per RFC 4180.
func NewLatin1Reader(r io.Reader, comma rune) *csv.Reader {
	cr := csv.NewReader(charmap.ISO8859_1.NewDecoder().Reader(r))
	configureStrict(cr, comma)
	return cr
}

// NewReader returns a strict CSV reader for UTF-8 files (e.g. local_votacao).
func NewReader(r io.Reader, comma rune) *csv.Reader {
	cr := csv.NewReader(r)
	configureStrict(cr, comma)
	return cr
}

func configureStrict(cr *csv.Reader, comma rune) {
	cr.Comma = comma
	cr.LazyQuotes = false
	cr.TrimLeadingSpace = true
}

// ReadHeader reads the first row, locks FieldsPerRecord to the header width,
// and builds a column-name → index map (uppercase, quotes stripped).
func ReadHeader(cr *csv.Reader, columnMapping map[string]string) ([]string, map[string]int, error) {
	rawHeader, err := cr.Read()
	if err != nil {
		return nil, nil, err
	}
	cr.FieldsPerRecord = len(rawHeader)
	return rawHeader, BuildColumnIndexes(rawHeader, columnMapping), nil
}

// BuildColumnIndexes maps normalized column names to their positions in the header row.
func BuildColumnIndexes(rawHeader []string, columnMapping map[string]string) map[string]int {
	colIndexes := make(map[string]int, len(rawHeader))
	for i, col := range rawHeader {
		name := strings.ToUpper(strings.Trim(col, `"`))
		if columnMapping != nil {
			if mapped, ok := columnMapping[name]; ok {
				name = mapped
			}
		}
		colIndexes[name] = i
	}
	return colIndexes
}

// IsNullSentinel reports TSE null markers.
func IsNullSentinel(v string) bool {
	v = strings.TrimSpace(v)
	return v == "" || v == "#NULO#" || v == "#NULO"
}

// Cell returns a trimmed cell value or empty string when out of range.
func Cell(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

// CellPtr returns nil for empty/TSE-null cells, otherwise a pointer to the trimmed value.
func CellPtr(record []string, idx int) interface{} {
	v := Cell(record, idx)
	if IsNullSentinel(v) {
		return nil
	}
	return v
}

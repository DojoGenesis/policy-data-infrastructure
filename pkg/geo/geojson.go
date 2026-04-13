package geo

import (
	"encoding/json"
	"fmt"
	"io"
)

// Feature represents a single GeoJSON Feature object. The Geometry field
// holds the raw JSON of the geometry so that callers can work with any
// geometry type without requiring a geometry-specific struct.
type Feature struct {
	Type       string                 `json:"type"`
	Geometry   json.RawMessage        `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// FeatureCollection represents a GeoJSON FeatureCollection object.
type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

// ReadFeatureCollection decodes a GeoJSON FeatureCollection from r. It returns
// an error if the JSON is malformed or the top-level type is not
// "FeatureCollection".
func ReadFeatureCollection(r io.Reader) (*FeatureCollection, error) {
	var fc FeatureCollection
	dec := json.NewDecoder(r)
	if err := dec.Decode(&fc); err != nil {
		return nil, fmt.Errorf("geo: decoding FeatureCollection: %w", err)
	}
	if fc.Type != "FeatureCollection" {
		return nil, fmt.Errorf("geo: expected type \"FeatureCollection\", got %q", fc.Type)
	}
	return &fc, nil
}

// WriteFeatureCollection encodes fc to w as JSON. The output is a single-line
// JSON object (no pretty-printing). Returns an error if encoding or writing
// fails.
func WriteFeatureCollection(w io.Writer, fc *FeatureCollection) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(fc); err != nil {
		return fmt.Errorf("geo: encoding FeatureCollection: %w", err)
	}
	return nil
}

// geoidPropertyKeys is the ordered list of property key names that are checked
// when looking for a GEOID value. The first key with a non-empty string value
// wins.
var geoidPropertyKeys = []string{
	"GEOID",
	"geoid",
	"GEOID10",
	"GEOID20",
	"GEO_ID",
}

// GetGEOID extracts a GEOID string from a Feature's properties. It checks a
// set of well-known property keys in order. Returns an empty string when no
// recognised GEOID property is found or when f is nil.
func (fc *FeatureCollection) GetGEOID(f *Feature) string {
	if f == nil {
		return ""
	}
	for _, key := range geoidPropertyKeys {
		if val, ok := f.Properties[key]; ok {
			if s, ok := val.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

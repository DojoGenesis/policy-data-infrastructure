package store

import "encoding/json"

// marshalJSONB converts a map to a raw JSON byte slice suitable for a pgx
// JSONB parameter. Returns nil (SQL NULL) when the map is nil or empty.
// Returns a JSON-encoded error object if marshaling fails, so the caller never
// silently stores NULL for a non-empty input.
func marshalJSONB(m map[string]interface{}) []byte {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		// Return a diagnostic object so we don't silently lose data.
		return []byte(`{"_marshal_error":"` + err.Error() + `"}`)
	}
	return b
}

// unmarshalJSONB parses a raw JSON byte slice returned by pgx into a
// map[string]interface{}. Returns nil when b is nil or empty.
func unmarshalJSONB(b []byte) map[string]interface{} {
	if len(b) == 0 {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

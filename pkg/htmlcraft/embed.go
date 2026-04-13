// Package htmlcraft converts policy data into self-contained HTML deliverables.
// All data is embedded as a window.__DATA__ JavaScript global so that Web
// Components can reference datasets by key without any external requests.
package htmlcraft

import (
	"encoding/json"
	"fmt"
)

// DataSet is a named collection of data for embedding in the HTML document.
type DataSet struct {
	Key  string      // e.g. "indicators", "geojson", "analysis_scores"
	Data interface{} // will be JSON-marshaled
}

// EmbedData generates a <script> block that sets window.__DATA__ to a map
// of all provided datasets keyed by their Key field.
func EmbedData(datasets []DataSet) (string, error) {
	dataMap := make(map[string]interface{}, len(datasets))
	for _, ds := range datasets {
		dataMap[ds.Key] = ds.Data
	}
	b, err := json.Marshal(dataMap)
	if err != nil {
		return "", fmt.Errorf("htmlcraft: marshal data: %w", err)
	}
	return fmt.Sprintf(`<script>window.__DATA__ = %s;</script>`, string(b)), nil
}

package geo

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const sampleFeatureCollection = `{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [-89.3838, 43.0731]
      },
      "properties": {
        "GEOID": "55025002100",
        "name": "Tract 21"
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [-90.0, 44.0]
      },
      "properties": {
        "geoid": "55025002200",
        "name": "Tract 22"
      }
    }
  ]
}`

func TestReadFeatureCollection(t *testing.T) {
	fc, err := ReadFeatureCollection(strings.NewReader(sampleFeatureCollection))
	if err != nil {
		t.Fatalf("ReadFeatureCollection: unexpected error: %v", err)
	}
	if fc.Type != "FeatureCollection" {
		t.Errorf("fc.Type = %q, want \"FeatureCollection\"", fc.Type)
	}
	if len(fc.Features) != 2 {
		t.Fatalf("len(fc.Features) = %d, want 2", len(fc.Features))
	}
	if fc.Features[0].Type != "Feature" {
		t.Errorf("features[0].Type = %q, want \"Feature\"", fc.Features[0].Type)
	}
}

func TestReadFeatureCollectionErrors(t *testing.T) {
	// Malformed JSON.
	if _, err := ReadFeatureCollection(strings.NewReader("{bad json")); err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}

	// Wrong top-level type.
	wrong := `{"type":"Feature","geometry":null,"properties":{}}`
	if _, err := ReadFeatureCollection(strings.NewReader(wrong)); err == nil {
		t.Error("expected error for non-FeatureCollection type, got nil")
	}
}

func TestWriteFeatureCollection(t *testing.T) {
	original, err := ReadFeatureCollection(strings.NewReader(sampleFeatureCollection))
	if err != nil {
		t.Fatalf("ReadFeatureCollection: %v", err)
	}

	var buf bytes.Buffer
	if err := WriteFeatureCollection(&buf, original); err != nil {
		t.Fatalf("WriteFeatureCollection: unexpected error: %v", err)
	}

	// Re-read what was written and compare.
	roundTripped, err := ReadFeatureCollection(&buf)
	if err != nil {
		t.Fatalf("ReadFeatureCollection (round-trip): %v", err)
	}

	if roundTripped.Type != original.Type {
		t.Errorf("round-trip Type = %q, want %q", roundTripped.Type, original.Type)
	}
	if len(roundTripped.Features) != len(original.Features) {
		t.Errorf("round-trip Features count = %d, want %d", len(roundTripped.Features), len(original.Features))
	}
}

func TestRoundTripPreservesProperties(t *testing.T) {
	original, err := ReadFeatureCollection(strings.NewReader(sampleFeatureCollection))
	if err != nil {
		t.Fatalf("ReadFeatureCollection: %v", err)
	}

	var buf bytes.Buffer
	if err := WriteFeatureCollection(&buf, original); err != nil {
		t.Fatalf("WriteFeatureCollection: %v", err)
	}

	rt, err := ReadFeatureCollection(&buf)
	if err != nil {
		t.Fatalf("ReadFeatureCollection (round-trip): %v", err)
	}

	// First feature should have GEOID "55025002100".
	if geoid := rt.GetGEOID(&rt.Features[0]); geoid != "55025002100" {
		t.Errorf("round-trip features[0] GEOID = %q, want \"55025002100\"", geoid)
	}
}

func TestRoundTripPreservesGeometry(t *testing.T) {
	original, err := ReadFeatureCollection(strings.NewReader(sampleFeatureCollection))
	if err != nil {
		t.Fatalf("ReadFeatureCollection: %v", err)
	}

	var buf bytes.Buffer
	if err := WriteFeatureCollection(&buf, original); err != nil {
		t.Fatalf("WriteFeatureCollection: %v", err)
	}

	rt, err := ReadFeatureCollection(&buf)
	if err != nil {
		t.Fatalf("ReadFeatureCollection (round-trip): %v", err)
	}

	// Decode the geometry from the original and the round-tripped version and
	// compare their JSON representations.
	var origGeom, rtGeom interface{}
	if err := json.Unmarshal(original.Features[0].Geometry, &origGeom); err != nil {
		t.Fatalf("unmarshal original geometry: %v", err)
	}
	if err := json.Unmarshal(rt.Features[0].Geometry, &rtGeom); err != nil {
		t.Fatalf("unmarshal round-trip geometry: %v", err)
	}

	origBytes, _ := json.Marshal(origGeom)
	rtBytes, _ := json.Marshal(rtGeom)
	if string(origBytes) != string(rtBytes) {
		t.Errorf("geometry mismatch after round-trip:\n  original: %s\n  got:      %s", origBytes, rtBytes)
	}
}

func TestGetGEOID(t *testing.T) {
	fc := &FeatureCollection{}

	// Nil feature.
	if g := fc.GetGEOID(nil); g != "" {
		t.Errorf("GetGEOID(nil) = %q, want \"\"", g)
	}

	tests := []struct {
		name       string
		properties map[string]interface{}
		want       string
	}{
		{
			name:       "uppercase GEOID",
			properties: map[string]interface{}{"GEOID": "55025"},
			want:       "55025",
		},
		{
			name:       "lowercase geoid",
			properties: map[string]interface{}{"geoid": "55025"},
			want:       "55025",
		},
		{
			name:       "GEOID10 variant",
			properties: map[string]interface{}{"GEOID10": "55025002100"},
			want:       "55025002100",
		},
		{
			name:       "GEOID20 variant",
			properties: map[string]interface{}{"GEOID20": "55025002100"},
			want:       "55025002100",
		},
		{
			name:       "GEO_ID variant",
			properties: map[string]interface{}{"GEO_ID": "0400000US55"},
			want:       "0400000US55",
		},
		{
			name:       "uppercase wins over lowercase",
			properties: map[string]interface{}{"GEOID": "55025", "geoid": "99999"},
			want:       "55025",
		},
		{
			name:       "no recognised key",
			properties: map[string]interface{}{"name": "Dane County"},
			want:       "",
		},
		{
			name:       "empty properties",
			properties: map[string]interface{}{},
			want:       "",
		},
		{
			name:       "non-string value ignored",
			properties: map[string]interface{}{"GEOID": 55025},
			want:       "",
		},
		{
			name:       "empty string value ignored",
			properties: map[string]interface{}{"GEOID": "", "geoid": "55025"},
			want:       "55025",
		},
	}

	for _, tc := range tests {
		f := &Feature{
			Type:       "Feature",
			Properties: tc.properties,
		}
		got := fc.GetGEOID(f)
		if got != tc.want {
			t.Errorf("GetGEOID [%s]: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

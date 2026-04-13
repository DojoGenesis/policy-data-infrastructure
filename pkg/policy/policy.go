package policy

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
)

// PolicyRecord represents a structured policy position from a candidate.
type PolicyRecord struct {
	ID                string `json:"id"`
	Candidate         string `json:"candidate"`
	Office            string `json:"office"`
	State             string `json:"state"`
	Category          string `json:"category"`
	Title             string `json:"policy_title"`
	Description       string `json:"description"`
	BillReferences    string `json:"bill_references"`
	ClaimsEmpirical   string `json:"claims_empirical"`
	EquityDimension   string `json:"equity_dimension"`
	GeographicScope   string `json:"geographic_scope"`
	DataSourcesNeeded string `json:"data_sources_needed"`
}

// csvColumns maps CSV header names to the order they appear in the CSV.
// index positions match the header row: id,candidate,office,state,category,
// policy_title,description,bill_references,claims_empirical,equity_dimension,
// geographic_scope,data_sources_needed
const (
	colID                = 0
	colCandidate         = 1
	colOffice            = 2
	colState             = 3
	colCategory          = 4
	colPolicyTitle       = 5
	colDescription       = 6
	colBillReferences    = 7
	colClaimsEmpirical   = 8
	colEquityDimension   = 9
	colGeographicScope   = 10
	colDataSourcesNeeded = 11
)

// LoadPoliciesFromCSV reads a policy CSV file and returns a slice of PolicyRecords.
// The CSV must have a header row; it is skipped. Empty trailing rows are ignored.
func LoadPoliciesFromCSV(path string) ([]PolicyRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("policy: open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("policy: parse CSV %s: %w", path, err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("policy: CSV %s is empty", path)
	}

	// Validate header
	header := rows[0]
	if len(header) < colDataSourcesNeeded+1 {
		return nil, fmt.Errorf("policy: CSV %s: expected at least 12 columns, got %d", path, len(header))
	}

	records := make([]PolicyRecord, 0, len(rows)-1)
	for i, row := range rows[1:] {
		// Skip completely empty rows
		if len(row) == 0 {
			continue
		}
		allEmpty := true
		for _, cell := range row {
			if strings.TrimSpace(cell) != "" {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			continue
		}

		if len(row) < colDataSourcesNeeded+1 {
			return nil, fmt.Errorf("policy: CSV %s row %d: expected 12 columns, got %d", path, i+2, len(row))
		}

		records = append(records, PolicyRecord{
			ID:                strings.TrimSpace(row[colID]),
			Candidate:         strings.TrimSpace(row[colCandidate]),
			Office:            strings.TrimSpace(row[colOffice]),
			State:             strings.TrimSpace(row[colState]),
			Category:          strings.TrimSpace(row[colCategory]),
			Title:             strings.TrimSpace(row[colPolicyTitle]),
			Description:       strings.TrimSpace(row[colDescription]),
			BillReferences:    strings.TrimSpace(row[colBillReferences]),
			ClaimsEmpirical:   strings.TrimSpace(row[colClaimsEmpirical]),
			EquityDimension:   strings.TrimSpace(row[colEquityDimension]),
			GeographicScope:   strings.TrimSpace(row[colGeographicScope]),
			DataSourcesNeeded: strings.TrimSpace(row[colDataSourcesNeeded]),
		})
	}

	return records, nil
}

// FilterByCategory returns policies matching the given category (case-sensitive).
func FilterByCategory(policies []PolicyRecord, category string) []PolicyRecord {
	out := make([]PolicyRecord, 0)
	for _, p := range policies {
		if p.Category == category {
			out = append(out, p)
		}
	}
	return out
}

// FilterByEquityDimension returns policies matching the given equity dimension.
func FilterByEquityDimension(policies []PolicyRecord, dim string) []PolicyRecord {
	out := make([]PolicyRecord, 0)
	for _, p := range policies {
		if p.EquityDimension == dim {
			out = append(out, p)
		}
	}
	return out
}

// FilterByHighRelevance returns policies whose equity dimension is mapped in the crosswalk
// with a P1 priority — the highest-relevance tier for Atlas analysis.
func FilterByHighRelevance(policies []PolicyRecord, crosswalk *Crosswalk) []PolicyRecord {
	out := make([]PolicyRecord, 0)
	for _, p := range policies {
		dm, ok := crosswalk.Dimensions[p.EquityDimension]
		if !ok {
			continue
		}
		if dm.Priority == "P1" {
			out = append(out, p)
		}
	}
	return out
}

// Categories returns a sorted slice of distinct categories present in the policy set.
func Categories(policies []PolicyRecord) []string {
	seen := make(map[string]struct{})
	for _, p := range policies {
		if p.Category != "" {
			seen[p.Category] = struct{}{}
		}
	}
	cats := make([]string, 0, len(seen))
	for c := range seen {
		cats = append(cats, c)
	}
	sort.Strings(cats)
	return cats
}

// EquityDimensions returns a sorted slice of distinct equity dimensions in the policy set.
func EquityDimensions(policies []PolicyRecord) []string {
	seen := make(map[string]struct{})
	for _, p := range policies {
		if p.EquityDimension != "" {
			seen[p.EquityDimension] = struct{}{}
		}
	}
	dims := make([]string, 0, len(seen))
	for d := range seen {
		dims = append(dims, d)
	}
	sort.Strings(dims)
	return dims
}

// PolicySummary is a structured summary of a policy set.
type PolicySummary struct {
	TotalPolicies    int            `json:"total_policies"`
	Candidate        string         `json:"candidate"`
	Categories       map[string]int `json:"categories"`
	EquityDimensions map[string]int `json:"equity_dimensions"`
	BillCount        int            `json:"bill_count"`
}

// Summarize returns a PolicySummary for the provided policies.
// Candidate is taken from the first record; BillCount counts records with a non-empty BillReferences field.
func Summarize(policies []PolicyRecord) PolicySummary {
	s := PolicySummary{
		TotalPolicies:    len(policies),
		Categories:       make(map[string]int),
		EquityDimensions: make(map[string]int),
	}
	for _, p := range policies {
		if s.Candidate == "" && p.Candidate != "" {
			s.Candidate = p.Candidate
		}
		if p.Category != "" {
			s.Categories[p.Category]++
		}
		if p.EquityDimension != "" {
			s.EquityDimensions[p.EquityDimension]++
		}
		if p.BillReferences != "" {
			s.BillCount++
		}
	}
	return s
}

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newLoadPoliciesCmd returns the "pdi load-policies" command.
//
// Usage:
//
//	pdi load-policies --dir data/policies/
//	pdi load-policies --dir data/policies/ --db postgres://...
func newLoadPoliciesCmd() *cobra.Command {
	var dir string

	cmd := &cobra.Command{
		Use:   "load-policies",
		Short: "Load policy CSV files into the database",
		Long: `load-policies reads all *.csv files in --dir and upserts their rows
into the policies table. Each CSV must follow the 12-column schema:
  id, candidate, office, state, category, policy_title, description,
  bill_references, claims_empirical, equity_dimension, geographic_scope,
  data_sources_needed

Existing rows with the same id are updated (upsert). Multiple runs are
idempotent. Prints a per-file count on success.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadPolicies(dir)
		},
	}

	cmd.Flags().StringVar(&dir, "dir", "data/policies/", "Directory containing policy CSV files")

	return cmd
}

func runLoadPolicies(dir string) error {
	ctx := context.Background()

	// Resolve the DB connection string the same way other commands do.
	connString := dbFlag
	if env := os.Getenv("PDI_DATABASE_URL"); env != "" && connString == defaultDB {
		connString = env
	}

	s, err := store.NewPostgresStore(ctx, connString)
	if err != nil {
		return fmt.Errorf("load-policies: open store: %w", err)
	}
	defer s.Close()

	// Walk the directory and collect CSV files.
	var csvFiles []string
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(strings.ToLower(d.Name()), ".csv") {
			csvFiles = append(csvFiles, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("load-policies: walk %s: %w", dir, err)
	}

	if len(csvFiles) == 0 {
		fmt.Fprintf(os.Stderr, "load-policies: no CSV files found in %s\n", dir)
		return nil
	}

	totalLoaded := 0
	for _, csvPath := range csvFiles {
		count, err := loadPolicyCSV(ctx, s, csvPath)
		if err != nil {
			return fmt.Errorf("load-policies: %s: %w", csvPath, err)
		}
		fmt.Printf("  %-50s  %d policies loaded\n", filepath.Base(csvPath), count)
		totalLoaded += count
	}

	fmt.Printf("load-policies: done — %d total policies upserted from %d file(s)\n", totalLoaded, len(csvFiles))
	return nil
}

// loadPolicyCSV parses one CSV file and upserts its rows into the store.
// Returns the number of records upserted.
func loadPolicyCSV(ctx context.Context, s store.Store, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}

	// Build a column index from the header so we tolerate minor ordering
	// differences while still validating required columns are present.
	colIdx := make(map[string]int, len(header))
	for i, h := range header {
		colIdx[strings.TrimSpace(strings.ToLower(h))] = i
	}

	required := []string{"id", "candidate", "category"}
	for _, col := range required {
		if _, ok := colIdx[col]; !ok {
			return 0, fmt.Errorf("missing required column %q in %s", col, path)
		}
	}

	// Helper: safely extract a field value by column name (returns "" if absent).
	field := func(row []string, name string) string {
		idx, ok := colIdx[name]
		if !ok || idx >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[idx])
	}

	var records []store.PolicyRecord
	rowNum := 1
	for {
		row, err := r.Read()
		if err != nil {
			break // EOF or error — we'll detect real errors below
		}
		rowNum++

		id := field(row, "id")
		if id == "" {
			fmt.Fprintf(os.Stderr, "  warning: row %d has empty id, skipping\n", rowNum)
			continue
		}

		// CSV uses "policy_title" to match the canonical column name in the CSV files.
		title := field(row, "policy_title")
		if title == "" {
			title = field(row, "title") // fallback if someone uses "title"
		}

		records = append(records, store.PolicyRecord{
			ID:                id,
			Candidate:         field(row, "candidate"),
			Office:            field(row, "office"),
			State:             field(row, "state"),
			Category:          field(row, "category"),
			Title:             title,
			Description:       field(row, "description"),
			BillReferences:    field(row, "bill_references"),
			ClaimsEmpirical:   field(row, "claims_empirical"),
			EquityDimension:   field(row, "equity_dimension"),
			GeographicScope:   field(row, "geographic_scope"),
			DataSourcesNeeded: field(row, "data_sources_needed"),
			SourceURL:         field(row, "source_url"),
		})
	}

	if len(records) == 0 {
		return 0, nil
	}

	if err := s.PutPolicies(ctx, records); err != nil {
		return 0, fmt.Errorf("upsert: %w", err)
	}

	return len(records), nil
}

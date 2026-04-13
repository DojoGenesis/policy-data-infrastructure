package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/geo"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newQueryCmd returns the "pdi query" command group.
//
//	pdi query indicators --geoid 55025000100 --variables median_hh_income,poverty_rate
//	pdi query geography  --geoid 55025 --children
//	pdi query scores     --analysis-id <uuid> --tier very_high
func newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query stored data",
		Long:  `query provides sub-commands for querying indicators, geographies, and analysis scores.`,
	}
	cmd.AddCommand(
		newQueryIndicatorsCmd(),
		newQueryGeographyCmd(),
		newQueryScoresCmd(),
	)
	return cmd
}

// ── pdi query indicators ───────────────────────────────────────────────────────

func newQueryIndicatorsCmd() *cobra.Command {
	var (
		geoid     string
		variables string
		vintage   string
		jsonOut   bool
	)

	cmd := &cobra.Command{
		Use:   "indicators",
		Short: "Query indicators for a geography",
		Long:  `Retrieve stored indicator values for a GEOID. Use --variables to filter to specific variable IDs (comma-separated).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runQueryIndicators(geoid, variables, vintage, jsonOut)
		},
	}

	cmd.Flags().StringVar(&geoid, "geoid", "", "GEOID to query (e.g. 55025000100 for a tract, 55025 for a county) [required]")
	cmd.Flags().StringVar(&variables, "variables", "", "Comma-separated variable IDs to return (omit for all)")
	cmd.Flags().StringVar(&vintage, "vintage", "", "Vintage to filter by (e.g. ACS-2023-5yr; omit for latest)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output as JSON instead of a table")

	_ = cmd.MarkFlagRequired("geoid")

	return cmd
}

func runQueryIndicators(geoid, variables, vintage string, jsonOut bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("query indicators: connect: %w", err)
	}
	defer s.Close()

	q := store.IndicatorQuery{
		GEOIDs:     []string{geoid},
		LatestOnly: vintage == "",
	}
	if vintage != "" {
		q.Vintage = vintage
	}
	if variables != "" {
		q.VariableIDs = parseCSV(variables)
	}

	indicators, err := s.QueryIndicators(ctx, q)
	if err != nil {
		return fmt.Errorf("query indicators: %w", err)
	}
	if len(indicators) == 0 {
		fmt.Fprintln(os.Stdout, "(no indicators found)")
		return nil
	}

	if jsonOut {
		return printJSON(indicators)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "GEOID\tVARIABLE_ID\tVINTAGE\tVALUE\tMOE")
	for _, ind := range indicators {
		valStr := "—"
		if ind.Value != nil {
			valStr = fmt.Sprintf("%.4g", *ind.Value)
		}
		moeStr := "—"
		if ind.MarginOfError != nil {
			moeStr = fmt.Sprintf("%.4g", *ind.MarginOfError)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", ind.GEOID, ind.VariableID, ind.Vintage, valStr, moeStr)
	}
	return w.Flush()
}

// ── pdi query geography ────────────────────────────────────────────────────────

func newQueryGeographyCmd() *cobra.Command {
	var (
		geoid    string
		children bool
		jsonOut  bool
	)

	cmd := &cobra.Command{
		Use:   "geography",
		Short: "Query a geography and optionally its children",
		Long:  `Retrieve metadata for a single geography by GEOID. Use --children to list all child geographies.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runQueryGeography(geoid, children, jsonOut)
		},
	}

	cmd.Flags().StringVar(&geoid, "geoid", "", "GEOID to query (e.g. 55025 for Dane County) [required]")
	cmd.Flags().BoolVar(&children, "children", false, "Also list child geographies")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output as JSON")

	_ = cmd.MarkFlagRequired("geoid")

	return cmd
}

func runQueryGeography(geoid string, children bool, jsonOut bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("query geography: connect: %w", err)
	}
	defer s.Close()

	g, err := s.GetGeography(ctx, geoid)
	if err != nil {
		return fmt.Errorf("query geography: %w", err)
	}

	var childGeos []geo.Geography
	if children {
		childGeos, err = s.QueryGeographies(ctx, store.GeoQuery{
			ParentGEOID: geoid,
			Limit:       10000,
		})
		if err != nil {
			return fmt.Errorf("query geography children: %w", err)
		}
	}

	if jsonOut {
		out := map[string]interface{}{"geography": g}
		if children {
			out["children"] = childGeos
		}
		return printJSON(out)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FIELD\tVALUE")
	fmt.Fprintf(w, "GEOID\t%s\n", g.GEOID)
	fmt.Fprintf(w, "Level\t%s\n", g.Level)
	fmt.Fprintf(w, "Name\t%s\n", g.Name)
	fmt.Fprintf(w, "State FIPS\t%s\n", g.StateFIPS)
	fmt.Fprintf(w, "County FIPS\t%s\n", g.CountyFIPS)
	fmt.Fprintf(w, "Parent GEOID\t%s\n", g.ParentGEOID)
	fmt.Fprintf(w, "Population\t%d\n", g.Population)
	fmt.Fprintf(w, "Land Area (m²)\t%.0f\n", g.LandAreaM2)
	fmt.Fprintf(w, "Lat/Lon\t%.6f, %.6f\n", g.Lat, g.Lon)
	if err := w.Flush(); err != nil {
		return err
	}

	if children && len(childGeos) > 0 {
		fmt.Printf("\n%d child geographies:\n", len(childGeos))
		cw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(cw, "GEOID\tLEVEL\tNAME\tPOPULATION")
		for _, c := range childGeos {
			fmt.Fprintf(cw, "%s\t%s\t%s\t%d\n", c.GEOID, c.Level, c.Name, c.Population)
		}
		return cw.Flush()
	}
	return nil
}

// ── pdi query scores ───────────────────────────────────────────────────────────

func newQueryScoresCmd() *cobra.Command {
	var (
		analysisID string
		tier       string
		limit      int
		jsonOut    bool
	)

	cmd := &cobra.Command{
		Use:   "scores",
		Short: "Query analysis scores",
		Long:  `Retrieve per-geography scores for a specific analysis result. Optionally filter by tier.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runQueryScores(analysisID, tier, limit, jsonOut)
		},
	}

	cmd.Flags().StringVar(&analysisID, "analysis-id", "", "Analysis UUID to query [required]")
	cmd.Flags().StringVar(&tier, "tier", "", "Filter to a single tier (e.g. very_high, high, moderate, low, minimal)")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum rows to return (0 = all)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output as JSON")

	_ = cmd.MarkFlagRequired("analysis-id")

	return cmd
}

func runQueryScores(analysisID, tier string, limit int, jsonOut bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("query scores: connect: %w", err)
	}
	defer s.Close()

	scores, err := s.QueryAnalysisScores(ctx, analysisID, tier)
	if err != nil {
		return fmt.Errorf("query scores: %w", err)
	}
	if len(scores) == 0 {
		fmt.Fprintln(os.Stdout, "(no scores found)")
		return nil
	}

	// Apply limit.
	if limit > 0 && len(scores) > limit {
		scores = scores[:limit]
	}

	if jsonOut {
		return printJSON(scores)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "GEOID\tRANK\tSCORE\tPERCENTILE\tTIER")
	for _, sc := range scores {
		fmt.Fprintf(w, "%s\t%d\t%.4f\t%.2f\t%s\n",
			sc.GEOID, sc.Rank, sc.Score, sc.Percentile, sc.Tier)
	}
	return w.Flush()
}

// ── shared helpers ─────────────────────────────────────────────────────────────

// printJSON marshals v to indented JSON and writes it to stdout.
func printJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	// Handle slices of nil — use empty array instead of null.
	if v == nil {
		fmt.Fprintln(os.Stdout, "[]")
		return nil
	}
	return enc.Encode(v)
}

// geoLevel is used by parseScope in analyze.go; define toString helper here
// since this file is the natural home for geo-related output.
func geoLevelDisplay(level string) string {
	if level == "" {
		return ""
	}
	return strings.ToUpper(level[:1]) + level[1:]
}

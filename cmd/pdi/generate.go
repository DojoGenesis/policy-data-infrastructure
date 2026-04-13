package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/htmlcraft"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/narrative"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

// newGenerateCmd returns the "pdi generate" command group.
//
//	pdi generate narrative   --template five_mornings --scope county:55025 --analysis-id <uuid>
//	pdi generate deliverable --template five_mornings --scope county:55025 --analysis-id <uuid> --output atlas.html
func newGenerateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate narratives and HTML deliverables",
		Long: `generate provides sub-commands for rendering narrative documents and
self-contained HTML deliverables from stored analysis results.`,
	}
	cmd.AddCommand(
		newGenerateNarrativeCmd(),
		newGenerateDeliverableCmd(),
	)
	return cmd
}

// ── pdi generate narrative ────────────────────────────────────────────────────

func newGenerateNarrativeCmd() *cobra.Command {
	var (
		tmpl       string
		scope      string
		analysisID string
		selection  string
		count      int
		output     string
	)

	cmd := &cobra.Command{
		Use:   "narrative",
		Short: "Generate a narrative document from an analysis",
		Long: `Renders a narrative from stored analysis scores using the specified template.
The narrative is written to --output (or stdout when omitted).

Selection strategies:
  by_tier    — one geography per risk tier, most severe first (default)
  outliers   — geographies that over- or under-perform their structural predictions
  by_indicator — geographies with extreme values on a single indicator`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerateNarrative(tmpl, scope, analysisID, selection, count, output)
		},
	}

	cmd.Flags().StringVar(&tmpl, "template", "five_mornings", "Template name (e.g. five_mornings, equity_profile)")
	cmd.Flags().StringVar(&scope, "scope", "", "Geographic scope — format: level:geoid (e.g. county:55025) [required]")
	cmd.Flags().StringVar(&analysisID, "analysis-id", "", "Analysis UUID to draw profiles from [required]")
	cmd.Flags().StringVar(&selection, "selection", "by_tier", "Profile selection strategy: by_tier, outliers, by_indicator")
	cmd.Flags().IntVar(&count, "count", 5, "Number of geography profiles to include")
	cmd.Flags().StringVar(&output, "output", "", "Output file path (default: stdout)")

	_ = cmd.MarkFlagRequired("scope")
	_ = cmd.MarkFlagRequired("analysis-id")

	return cmd
}

func runGenerateNarrative(tmplName, scope, analysisID, selection string, count int, output string) error {
	_, scopeGEOID, err := parseScope(scope)
	if err != nil {
		return fmt.Errorf("generate narrative: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("generate narrative: connect: %w", err)
	}
	defer s.Close()

	// Build the narrative engine and load embedded templates.
	eng := narrative.NewEngine(s)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		// Non-fatal: engine will fall back to the built-in template.
		fmt.Fprintf(os.Stderr, "generate narrative: template load warning: %v\n", err)
	}

	// Build a human-readable scope name from the GEOID if we can.
	scopeName := scopeGEOID
	if g, err := s.GetGeography(ctx, scopeGEOID); err == nil && g != nil {
		scopeName = g.Name
	}

	doc, err := eng.Generate(ctx, narrative.GenerateRequest{
		Template:     tmplName,
		ScopeGEOID:  scopeGEOID,
		ScopeName:   scopeName,
		AnalysisID:  analysisID,
		ChapterCount: count,
		Selection:   selection,
	})
	if err != nil {
		return fmt.Errorf("generate narrative: %w", err)
	}

	html, err := eng.RenderHTML(doc)
	if err != nil {
		return fmt.Errorf("generate narrative: render: %w", err)
	}

	return writeOutput(output, []byte(html))
}

// ── pdi generate deliverable ─────────────────────────────────────────────────

func newGenerateDeliverableCmd() *cobra.Command {
	var (
		tmpl          string
		scope         string
		analysisID    string
		output        string
		title         string
		includeMap    bool
		includeCharts bool
		tileLayer     string
		components    string
		selection     string
		count         int
	)

	cmd := &cobra.Command{
		Use:   "deliverable",
		Short: "Generate a self-contained HTML deliverable",
		Long: `Renders a full single-file HTML policy brief combining a narrative, embedded
data, optional Leaflet map, and optional Chart.js charts.

The output file is self-contained and can be shared directly without any server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerateDeliverable(
				tmpl, scope, analysisID, output, title,
				includeMap, includeCharts, tileLayer,
				parseCSV(components), selection, count,
			)
		},
	}

	cmd.Flags().StringVar(&tmpl, "template", "five_mornings", "Narrative template name")
	cmd.Flags().StringVar(&scope, "scope", "", "Geographic scope — format: level:geoid [required]")
	cmd.Flags().StringVar(&analysisID, "analysis-id", "", "Analysis UUID to draw profiles from [required]")
	cmd.Flags().StringVar(&output, "output", "deliverable.html", "Output file path")
	cmd.Flags().StringVar(&title, "title", "", "Document title (default: derived from scope)")
	cmd.Flags().BoolVar(&includeMap, "map", false, "Embed a Leaflet choropleth map")
	cmd.Flags().BoolVar(&includeCharts, "charts", false, "Embed Chart.js visualisations")
	cmd.Flags().StringVar(&tileLayer, "tile-layer", "light", "Map basemap: light, dark, or satellite")
	cmd.Flags().StringVar(&components, "components", "metric-card,stat-callout,data-table", "Comma-separated Web Components to inline")
	cmd.Flags().StringVar(&selection, "selection", "by_tier", "Profile selection strategy")
	cmd.Flags().IntVar(&count, "count", 5, "Number of geography profiles to include")

	_ = cmd.MarkFlagRequired("scope")
	_ = cmd.MarkFlagRequired("analysis-id")

	return cmd
}

func runGenerateDeliverable(
	tmplName, scope, analysisID, output, titleFlag string,
	includeMap, includeCharts bool,
	tileLayer string,
	components []string,
	selection string,
	count int,
) error {
	_, scopeGEOID, err := parseScope(scope)
	if err != nil {
		return fmt.Errorf("generate deliverable: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("generate deliverable: connect: %w", err)
	}
	defer s.Close()

	// Resolve scope name.
	scopeName := scopeGEOID
	if g, err := s.GetGeography(ctx, scopeGEOID); err == nil && g != nil {
		scopeName = g.Name
	}

	docTitle := titleFlag
	if docTitle == "" {
		docTitle = "Five Mornings in " + scopeName
	}

	// Step 1: generate the narrative HTML fragment.
	eng := narrative.NewEngine(s)
	if err := eng.LoadEmbeddedTemplates(); err != nil {
		fmt.Fprintf(os.Stderr, "generate deliverable: template load warning: %v\n", err)
	}

	doc, err := eng.Generate(ctx, narrative.GenerateRequest{
		Template:     tmplName,
		ScopeGEOID:  scopeGEOID,
		ScopeName:   scopeName,
		AnalysisID:  analysisID,
		ChapterCount: count,
		Selection:   selection,
	})
	if err != nil {
		return fmt.Errorf("generate deliverable: narrative: %w", err)
	}

	narrativeHTML, err := eng.RenderHTML(doc)
	if err != nil {
		return fmt.Errorf("generate deliverable: narrative render: %w", err)
	}

	// Step 2: wrap in a full HTMLCraft deliverable.
	bridge := htmlcraft.NewBridge(s)
	opts := htmlcraft.DeliverableOpts{
		Title:         docTitle,
		IncludeMap:    includeMap,
		IncludeCharts: includeCharts,
		TileLayer:     tileLayer,
		Components:    components,
	}

	fullHTML, err := bridge.BuildDeliverable(ctx, narrativeHTML, scopeGEOID, opts)
	if err != nil {
		return fmt.Errorf("generate deliverable: build: %w", err)
	}

	if err := writeOutput(output, []byte(fullHTML)); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "generate deliverable: written to %s (%d bytes)\n", output, len(fullHTML))
	return nil
}

// ── shared I/O helpers ─────────────────────────────────────────────────────────

// writeOutput writes data to path, or to stdout when path is empty or "-".
func writeOutput(path string, data []byte) error {
	if path == "" || path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

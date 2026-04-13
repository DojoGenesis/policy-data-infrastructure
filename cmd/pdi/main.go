// Command pdi is the CLI for the policy data infrastructure platform.
// It provides subcommands for managing migrations, fetching source data,
// running analyses, querying indicators, generating narratives, serving the
// HTTP API, and orchestrating full ingest pipelines.
package main

import (
	"fmt"
	"os"

	"github.com/DojoGenesis/policy-data-infrastructure/internal/version"
	"github.com/spf13/cobra"
)

const defaultDB = "postgres://pdi:pdi@localhost:5432/pdi?sslmode=disable"

// dbFlag holds the value of the global --db flag.
var dbFlag string

// rootCmd is the top-level cobra command ("pdi").
var rootCmd = &cobra.Command{
	Use:   "pdi",
	Short: "Policy Data Infrastructure CLI",
	Long: `pdi manages the policy data infrastructure platform.

It provides commands for database migrations, data ingest, statistical
analysis, indicator queries, narrative generation, and serving the HTTP API.`,
	SilenceUsage: true,
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&dbFlag,
		"db",
		defaultDB,
		"PostgreSQL connection string (overrides PDI_DATABASE_URL)",
	)

	rootCmd.AddCommand(
		migrateCmd,
		newFetchCmd(),
		newAnalyzeCmd(),
		newQueryCmd(),
		newGenerateCmd(),
		newServeCmd(),
		newPipelineCmd(),
		newVersionCmd(),
	)
}

func main() {
	// Honour PDI_DATABASE_URL when --db is not explicitly provided.
	if env := os.Getenv("PDI_DATABASE_URL"); env != "" && dbFlag == defaultDB {
		dbFlag = env
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// --- stub subcommands ---

func newFetchCmd() *cobra.Command {
	var stateFIPS, countyFIPS string
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch source data from upstream APIs",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("fetch: state=%s county=%s (not yet implemented)\n", stateFIPS, countyFIPS)
			return nil
		},
	}
	cmd.Flags().StringVar(&stateFIPS, "state", "", "State FIPS code (e.g. 55)")
	cmd.Flags().StringVar(&countyFIPS, "county", "", "County FIPS code (e.g. 025)")
	return cmd
}

func newAnalyzeCmd() *cobra.Command {
	var scope string
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Run statistical analyses",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("analyze: scope=%s (not yet implemented)\n", scope)
			return nil
		},
	}
	cmd.Flags().StringVar(&scope, "scope", "", "Geographic scope, e.g. county:55025")
	return cmd
}

func newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query stored data",
	}
	cmd.AddCommand(newQueryIndicatorsCmd())
	return cmd
}

func newQueryIndicatorsCmd() *cobra.Command {
	var geoid string
	cmd := &cobra.Command{
		Use:   "indicators",
		Short: "Query indicators for a geography",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("query indicators: geoid=%s (not yet implemented)\n", geoid)
			return nil
		},
	}
	cmd.Flags().StringVar(&geoid, "geoid", "", "GEOID to query (e.g. 55025000100)")
	return cmd
}

func newGenerateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate narratives and deliverables",
	}
	cmd.AddCommand(newGenerateNarrativeCmd())
	return cmd
}

func newGenerateNarrativeCmd() *cobra.Command {
	var tmpl string
	cmd := &cobra.Command{
		Use:   "narrative",
		Short: "Generate a narrative from a template",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("generate narrative: template=%s (not yet implemented)\n", tmpl)
			return nil
		},
	}
	cmd.Flags().StringVar(&tmpl, "template", "", "Template name (e.g. five_mornings)")
	return cmd
}

func newServeCmd() *cobra.Command {
	var port int
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the HTTP API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("serve: port=%d (not yet implemented)\n", port)
			return nil
		},
	}
	cmd.Flags().IntVar(&port, "port", 8340, "Port to listen on")
	return cmd
}

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Orchestrate full ingest pipelines",
	}
	cmd.AddCommand(newPipelineRunCmd())
	return cmd
}

func newPipelineRunCmd() *cobra.Command {
	var county string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a full ingest pipeline for a county",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("pipeline run: county=%s (not yet implemented)\n", county)
			return nil
		},
	}
	cmd.Flags().StringVar(&county, "county", "", "County FIPS code (e.g. 55025)")
	return cmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("pdi %s (commit %s, built %s)\n",
				version.Version, version.Commit, version.Date)
		},
	}
}

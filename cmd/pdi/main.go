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

// newVersionCmd is the only stub still in main.go — all other commands
// are defined in their own files (fetch.go, analyze.go, query.go, serve.go,
// pipeline.go, generate.go, etc.)
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

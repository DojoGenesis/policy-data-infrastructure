package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
	"github.com/spf13/cobra"
)

// migrateCmd is the "pdi migrate" command group.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage database schema migrations",
}

func init() {
	migrateCmd.AddCommand(migrateUpCmd, migrateDownCmd)
}

var migrateUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all pending migrations",
	RunE:  runMigrateUp,
}

var migrateDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Roll back the last migration (not yet implemented)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("migrate down: not yet implemented")
	},
}

func runMigrateUp(cmd *cobra.Command, args []string) error {
	connString := resolveConnString()

	fmt.Fprintf(os.Stdout, "connecting to %s\n", sanitizeConnString(connString))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Now()

	s, err := store.NewPostgresStore(ctx, connString)
	if err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	defer s.Close()

	fmt.Fprintf(os.Stdout, "migrations applied successfully in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// resolveConnString returns the --db flag value, falling back to the
// PDI_DATABASE_URL environment variable, and finally to the hardcoded default.
func resolveConnString() string {
	if dbFlag != "" && dbFlag != defaultDB {
		return dbFlag
	}
	if env := os.Getenv("PDI_DATABASE_URL"); env != "" {
		return env
	}
	return dbFlag
}

// sanitizeConnString strips the password from a postgres connection string for
// display purposes. If parsing fails the original string is returned.
func sanitizeConnString(s string) string {
	// Simple approach: hide everything between :// and @ when there is a
	// user:pass section, replacing the password portion with *****.
	//
	// Example:
	//   postgres://pdi:secret@localhost:5432/pdi  →  postgres://pdi:*****@localhost:5432/pdi
	//   postgres://localhost:5432/pdi             →  postgres://localhost:5432/pdi
	const proto = "postgres://"
	if len(s) <= len(proto) {
		return s
	}
	rest := s[len(proto):]
	atIdx := -1
	for i, ch := range rest {
		if ch == '@' {
			atIdx = i
			break
		}
	}
	if atIdx < 0 {
		return s // no user info section
	}
	userInfo := rest[:atIdx]
	colonIdx := -1
	for i, ch := range userInfo {
		if ch == ':' {
			colonIdx = i
			break
		}
	}
	if colonIdx < 0 {
		return s // no password
	}
	user := userInfo[:colonIdx]
	return proto + user + ":*****@" + rest[atIdx+1:]
}

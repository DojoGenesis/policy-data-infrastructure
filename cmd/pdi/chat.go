package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/gateway"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/grounding"
)

const defaultBundleDir = "analysis/output/atlas"

// newChatCmd serves grounded question-answering over the Atlas bundle.
//
// Deliberately separate from `serve`: this path reads the shipped static bundle
// and needs no database, so it runs anywhere the bundle does. It also means the
// chat answers from exactly the dataset the Atlas pages render, which is the
// only way the two can be guaranteed not to contradict each other.
func newChatCmd() *cobra.Command {
	var (
		port   int
		dir    string
		ask    string
		asJSON bool
	)

	cmd := &cobra.Command{
		Use:   "chat",
		Short: "Grounded question-answering over the Atlas bundle",
		Long: `Answers questions about the Atlas data with figures taken from the data itself.

A model may plan the query and may write the prose, but never supplies a number:
the query is executed deterministically, and any number in a model-written answer
that the result does not contain causes that answer to be discarded (ADR-006).

Model lanes are optional. With none configured, structured queries still work and
natural-language questions are refused honestly rather than guessed at.
  OPENROUTER_API_KEY / OPENROUTER_MODEL   quality lane
  OLLAMA_BASE_URL    / OLLAMA_MODEL       cost lane

Examples:
  pdi chat --ask "Which counties have the highest poverty rate?"
  pdi chat --port 8341
  curl -s localhost:8341/v1/policy/chat/schema`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if ask != "" {
				return runChatOnce(dir, ask, asJSON)
			}
			return runChatServe(port, dir)
		},
	}

	cmd.Flags().IntVar(&port, "port", 8341, "Port to listen on")
	cmd.Flags().StringVar(&dir, "bundle", defaultBundleDir, "Atlas bundle directory")
	cmd.Flags().StringVar(&ask, "ask", "", "Answer one question and exit (no server)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "With --ask, print the full JSON answer")
	return cmd
}

func runChatOnce(dir, question string, asJSON bool) error {
	ds, err := grounding.Load(dir)
	if err != nil {
		return fmt.Errorf("chat: load bundle from %s: %w", dir, err)
	}
	e := &grounding.Engine{Dataset: ds, MaxPlanAttempts: 2}
	if or := grounding.OpenRouterFromEnv(); or != nil {
		e.Planner, e.Composer = or, or
	} else if ol := grounding.OllamaFromEnv(); ol != nil {
		e.Planner, e.Composer = ol, ol
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ans, err := e.Answer(ctx, question)
	if err != nil {
		return fmt.Errorf("chat: %w", err)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(ans)
	}
	fmt.Println(ans.Text)
	if len(ans.Violations) > 0 {
		fmt.Fprintf(os.Stderr, "\n[%s] the model draft was discarded; %d unsupported number(s):\n",
			ans.Source, len(ans.Violations))
		for _, v := range ans.Violations {
			fmt.Fprintf(os.Stderr, "  %s\n", v.Error())
		}
	}
	if !ans.Answered {
		os.Exit(2)
	}
	return nil
}

func runChatServe(port int, dir string) error {
	plugin, err := gateway.NewChatPlugin(dir)
	if err != nil {
		return fmt.Errorf("chat: load bundle from %s: %w", dir, err)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), gin.Logger())

	plugin.RegisterRoutes(r.Group("/v1/policy"))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "plugin": plugin.Name()})
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		fmt.Printf("grounded chat on :%d (bundle: %s)\n", port, dir)
		fmt.Printf("  POST /v1/policy/chat         natural-language question\n")
		fmt.Printf("  POST /v1/policy/chat/query   structured Intent, no model\n")
		fmt.Printf("  GET  /v1/policy/chat/schema  query vocabulary\n")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "chat: %v\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

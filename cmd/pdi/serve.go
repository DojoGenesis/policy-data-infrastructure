package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	"github.com/DojoGenesis/policy-data-infrastructure/pkg/gateway"
	"github.com/DojoGenesis/policy-data-infrastructure/pkg/store"
)

//go:embed all:frontend
var frontendFS embed.FS

func newServeCmd() *cobra.Command {
	var port int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the HTTP API server",
		Long:  `Starts the policy data infrastructure HTTP API server with REST endpoints for geography, indicators, analysis, and deliverable generation.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(port)
		},
	}

	cmd.Flags().IntVar(&port, "port", 8340, "Port to listen on")
	return cmd
}

func runServe(port int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := store.NewPostgresStore(ctx, resolveConnString())
	if err != nil {
		return fmt.Errorf("serve: connect to store: %w", err)
	}
	defer s.Close()

	plugin := gateway.NewPlugin(s)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	// CORS — allow browser clients from any policydatainfrastructure.com origin
	// plus localhost for development. Configurable via CORS_ORIGINS env var
	// (comma-separated list of allowed origins).
	allowedOrigins := []string{
		"https://policydatainfrastructure.com",
		"https://www.policydatainfrastructure.com",
		"https://api.policydatainfrastructure.com",
		"http://localhost:*",
		"http://127.0.0.1:*",
	}
	if extra := os.Getenv("CORS_ORIGINS"); extra != "" {
		for _, o := range strings.Split(extra, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				allowedOrigins = append(allowedOrigins, o)
			}
		}
	}
	r.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}))

	v1 := r.Group("/v1")
	policyGroup := v1.Group("/policy")
	plugin.RegisterRoutes(policyGroup)

	// Liveness check — always returns 200.
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness check — verifies database connectivity.
	r.GET("/readyz", func(c *gin.Context) {
		pingCtx, pingCancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer pingCancel()
		if err := s.Ping(pingCtx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	// Chat endpoint — calls Anthropic directly with a rich system prompt
	// grounded in the live data. The ANTHROPIC_API_KEY env var must be set.
	anthropicKey := os.Getenv("ANTHROPIC_API_KEY")
	r.POST("/v1/chat", func(c *gin.Context) {
		if anthropicKey == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chat not configured (ANTHROPIC_API_KEY not set)"})
			return
		}

		var req struct {
			Message      string `json:"message"`
			SystemPrompt string `json:"system_prompt"`
			SessionID    string `json:"session_id"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || req.Message == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "message is required"})
			return
		}

		systemPrompt := req.SystemPrompt
		if systemPrompt == "" {
			systemPrompt = "You are a helpful assistant."
		}

		// Build Anthropic Messages API request
		anthropicBody := fmt.Sprintf(`{
			"model": "claude-sonnet-4-20250514",
			"max_tokens": 2048,
			"system": %s,
			"messages": [{"role": "user", "content": %s}]
		}`,
			jsonEscapeString(systemPrompt),
			jsonEscapeString(req.Message),
		)

		proxyReq, err := http.NewRequestWithContext(c.Request.Context(), "POST",
			"https://api.anthropic.com/v1/messages",
			strings.NewReader(anthropicBody))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "build request failed"})
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("x-api-key", anthropicKey)
		proxyReq.Header.Set("anthropic-version", "2023-06-01")

		client := &http.Client{Timeout: 2 * time.Minute}
		resp, err := client.Do(proxyReq)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "anthropic unreachable", "detail": err.Error()})
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusOK {
			c.JSON(resp.StatusCode, gin.H{"error": "anthropic error", "detail": string(respBody[:minInt(len(respBody), 500)])})
			return
		}

		// Parse Anthropic response and return in our format
		var anthropicResp struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(respBody, &anthropicResp); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "parse response failed"})
			return
		}

		text := ""
		for _, block := range anthropicResp.Content {
			text += block.Text
		}

		c.JSON(http.StatusOK, gin.H{
			"type":    "complete",
			"content": text,
		})
	})
	if anthropicKey != "" {
		fmt.Println("  chat:     /v1/chat → Anthropic Claude (direct)")
	} else {
		fmt.Println("  chat:     /v1/chat → NOT CONFIGURED (set ANTHROPIC_API_KEY)")
	}

	// Serve embedded frontend static files.
	feFS, _ := fs.Sub(frontendFS, "frontend")
	indexHTML, _ := fs.ReadFile(feFS, "index.html")
	r.GET("/", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", indexHTML)
	})
	r.GET("/static/*filepath", gin.WrapH(http.StripPrefix("/static", http.FileServer(http.FS(feFS)))))

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // disabled for SSE streams
	}

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nshutting down...")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()

	fmt.Printf("pdi serving on 0.0.0.0%s\n", addr)
	fmt.Printf("  frontend: http://0.0.0.0%s/\n", addr)
	fmt.Printf("  API:      http://0.0.0.0%s/v1/policy/\n", addr)
	fmt.Printf("  health:   http://0.0.0.0%s/health\n", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// jsonEscapeString returns a JSON-encoded string (with surrounding quotes).
func jsonEscapeString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// min returns the smaller of two ints. (Go 1.21+ has builtin min but we keep compat.)
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

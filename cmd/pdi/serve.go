package main

import (
	"context"
	"embed"
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

	// Chat proxy — forward /v1/chat to the Dojo Gateway for LLM-powered
	// conversational data analysis. The gateway handles model routing, tool
	// calling, and SSE streaming. DOJO_GATEWAY_URL configures the upstream.
	gatewayURL := os.Getenv("DOJO_GATEWAY_URL")
	if gatewayURL == "" {
		gatewayURL = "http://localhost:7340"
	}
	gwTarget := strings.TrimRight(gatewayURL, "/")
	r.POST("/v1/chat", func(c *gin.Context) {
		// Read the incoming request body.
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "read body failed"})
			return
		}
		// Forward to gateway /chat endpoint.
		proxyReq, err := http.NewRequestWithContext(c.Request.Context(), "POST", gwTarget+"/v1/chat", strings.NewReader(string(body)))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "build proxy request failed"})
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("Accept", c.GetHeader("Accept"))

		client := &http.Client{Timeout: 5 * time.Minute}
		resp, err := client.Do(proxyReq)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "gateway unreachable", "detail": err.Error()})
			return
		}
		defer resp.Body.Close()

		// Stream the response back — works for both JSON and SSE.
		extraHeaders := map[string]string{}
		for _, h := range []string{"Content-Type", "Cache-Control", "Connection"} {
			if v := resp.Header.Get(h); v != "" {
				extraHeaders[h] = v
			}
		}
		c.DataFromReader(resp.StatusCode, resp.ContentLength, resp.Header.Get("Content-Type"), resp.Body, extraHeaders)
	})
	fmt.Printf("  chat:     /v1/chat → %s/v1/chat\n", gwTarget)

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

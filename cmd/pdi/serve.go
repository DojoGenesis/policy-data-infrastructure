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

	// Seed evidence cards from embedded JSON on first startup.
	if seedFS, err := fs.Sub(frontendFS, "frontend"); err == nil {
		if evidenceJSON, err2 := fs.ReadFile(seedFS, "evidence_cards.json"); err2 == nil {
			if err2 := s.SeedEvidenceCardsFromJSON(ctx, evidenceJSON); err2 != nil {
				fmt.Printf("  evidence-cards seed: %v\n", err2)
			} else {
				fmt.Println("  evidence-cards: seeded from embedded JSON")
			}
		}
	}

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

	// Chat proxy — forward /v1/chat to the Dojo Gateway, which now supports
	// system_prompt passthrough (fixed in Gateway v3.2.2). The frontend's
	// ChatAdapter builds a rich system prompt from live API data and passes
	// it through the Gateway to Claude.
	gatewayURL := os.Getenv("DOJO_GATEWAY_URL")
	if gatewayURL == "" {
		gatewayURL = "http://localhost:7340"
	}
	gwTarget := strings.TrimRight(gatewayURL, "/")

	// Service credential for the upstream Gateway, read once at startup with
	// the same env-var pattern as DOJO_GATEWAY_URL above. The Gateway requires
	// `Authorization: Bearer <jwt>` across its whole /v1 group, so forwarding
	// without one is a guaranteed 401 whose body is the *Gateway's* error, not
	// PDI's — which is precisely how a broken chat shipped unnoticed.
	//
	// Never log the value. Only its presence is ever printed or reported.
	gwToken := strings.TrimSpace(os.Getenv("DOJO_GATEWAY_TOKEN"))

	r.POST("/v1/chat", newChatProxyHandler(gwTarget, gwToken, &http.Client{Timeout: 3 * time.Minute}))
	authState := "no credential — DOJO_GATEWAY_TOKEN unset, chat returns 503"
	if gwToken != "" {
		authState = "service credential configured (DOJO_GATEWAY_TOKEN)"
	}
	fmt.Printf("  chat:     /v1/chat → %s/v1/chat [%s]\n", gwTarget, authState)

	// Serve embedded frontend static files.
	feFS, _ := fs.Sub(frontendFS, "frontend")
	indexHTML, _ := fs.ReadFile(feFS, "index.html")
	r.GET("/", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", indexHTML)
	})
	r.GET("/static/*filepath", gin.WrapH(http.StripPrefix("/static", http.FileServer(http.FS(feFS)))))

	// Service Worker — must be served from root for correct scope.
	swJS, swErr := fs.ReadFile(feFS, "sw.js")
	r.GET("/sw.js", func(c *gin.Context) {
		if swErr != nil {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", swJS)
	})

	// Web App Manifest — PWA install support.
	manifestJSON, mfErr := fs.ReadFile(feFS, "manifest.json")
	r.GET("/manifest.json", func(c *gin.Context) {
		if mfErr != nil {
			c.String(http.StatusNotFound, "not found")
			return
		}
		c.Data(http.StatusOK, "application/manifest+json; charset=utf-8", manifestJSON)
	})

	// Clean URL routing: /page → page.html (no .html extension in URL).
	htmlPages := []struct{ route, file string }{
		{"/county", "county.html"},
		{"/compare", "compare.html"},
		{"/evidence", "evidence.html"},
		{"/candidates", "candidates.html"},
		{"/map", "map.html"},
		{"/narrative", "narrative.html"},
		{"/about", "about.html"},
		{"/composite", "composites.html"},
		{"/chat", "chat.html"},
	}
	for _, p := range htmlPages {
		p := p // capture by value for closure
		html, err := fs.ReadFile(feFS, p.file)
		if err != nil {
			continue // file not yet built; skip route
		}
		r.GET(p.route, func(c *gin.Context) {
			c.Data(http.StatusOK, "text/html; charset=utf-8", html)
		})
	}

	// Spanish language routes: /es/<page> serves the same page as English.
	// The lang-toggle.js client script detects the /es/ path prefix and
	// initializes to Spanish. ES twin files are no longer needed (ADR-011).
	r.GET("/es/*page", func(c *gin.Context) {
		page := strings.TrimPrefix(c.Param("page"), "/")
		if page == "" {
			page = "index"
		}
		html, err := fs.ReadFile(feFS, page+".html")
		if err != nil {
			c.String(http.StatusNotFound, "page not found")
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", html)
	})

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

// newChatProxyHandler builds the /v1/chat proxy handler.
//
// It is a constructor rather than an inline closure so the auth behaviour can be
// exercised against a stub upstream without standing up Postgres — the header
// logic is the part that broke in production, so it has to be testable on its own.
//
// Credential precedence:
//  1. An inbound client `Authorization` header, forwarded verbatim. Nothing sends
//     one today, but when authenticated visitors exist the Gateway should see
//     *their* identity for quota and audit purposes, not a shared service
//     credential silently substituted underneath them. PDI never inspects,
//     rewrites, or stores the value; the Gateway remains the only validator, so a
//     bad client token correctly yields the Gateway's 401 rather than being
//     quietly upgraded to full service access.
//  2. The DOJO_GATEWAY_TOKEN service credential — the anonymous-visitor path, and
//     the one that carries every request on the live site today.
//  3. Neither: fail fast with PDI's own ErrorResponse. Forwarding an
//     unauthenticated request only produces an upstream 401 whose body looks like
//     the Gateway rejecting the *user*, which is the confusion that let this bug
//     live in production.
func newChatProxyHandler(gwTarget, gwToken string, client *http.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		authz := strings.TrimSpace(c.GetHeader("Authorization"))
		if authz == "" && gwToken != "" {
			authz = "Bearer " + gwToken
		}
		if authz == "" {
			// 503, not 401: the visitor did nothing wrong — the server is
			// missing configuration. Distinguishable from an upstream 401 by
			// both status and the absence of the Gateway's "success" field.
			c.JSON(http.StatusServiceUnavailable, gateway.ErrorResponse{
				Error:  "chat is not configured on this server",
				Detail: "DOJO_GATEWAY_TOKEN is not set, so PDI holds no credential for the Dojo Gateway. No request was sent upstream. An operator must configure the service credential before chat can answer.",
			})
			return
		}

		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gateway.ErrorResponse{Error: "read body failed"})
			return
		}
		proxyReq, err := http.NewRequestWithContext(c.Request.Context(), "POST",
			gwTarget+"/v1/chat", strings.NewReader(string(body)))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gateway.ErrorResponse{Error: "build proxy request failed"})
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("Accept", c.GetHeader("Accept"))
		proxyReq.Header.Set("Authorization", authz)

		resp, err := client.Do(proxyReq)
		if err != nil {
			// err may embed the request URL but never a header value.
			c.JSON(http.StatusBadGateway, gateway.ErrorResponse{
				Error:  "gateway unreachable",
				Detail: err.Error(),
			})
			return
		}
		defer resp.Body.Close()

		extraHeaders := map[string]string{}
		for _, h := range []string{"Content-Type", "Cache-Control", "Connection"} {
			if v := resp.Header.Get(h); v != "" {
				extraHeaders[h] = v
			}
		}
		c.DataFromReader(resp.StatusCode, resp.ContentLength, resp.Header.Get("Content-Type"), resp.Body, extraHeaders)
	}
}

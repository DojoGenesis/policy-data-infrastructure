package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
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
	// Drain the queued-analysis runs (ADR-014 D3) for the server's lifetime.
	plugin.StartRunner(ctx)

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

	// Trusted proxies. gin's default is to trust EVERY peer (0.0.0.0/0 and
	// ::/0) with ForwardedByClientIP on, which makes c.ClientIP() return the
	// leftmost X-Forwarded-For entry — a value the caller controls end to end.
	// That is fine when nothing depends on client identity and actively
	// dangerous the moment something does, which /v1/chat's per-client budget
	// now does.
	//
	// The live topology is Cloudflare → Caddy → this process on localhost:8340,
	// so the only legitimate proxy hop is loopback. Trusting exactly that makes
	// c.ClientIP() resolve to the real peer instead of to a header. Override
	// with PDI_TRUSTED_PROXIES (comma-separated CIDRs) when the reverse proxy
	// is not on the same host — a container network, for instance.
	trustedProxies := []string{"127.0.0.1/32", "::1/128"}
	if v := strings.TrimSpace(os.Getenv("PDI_TRUSTED_PROXIES")); v != "" {
		trustedProxies = nil
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				trustedProxies = append(trustedProxies, p)
			}
		}
	}
	if err := r.SetTrustedProxies(trustedProxies); err != nil {
		return fmt.Errorf("serve: trusted proxies %v: %w", trustedProxies, err)
	}

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

	// Grounded chat over the LIVE database (2026-08-08): every indicator
	// with data, both levels, per-indicator vintages, snapshot refreshed on
	// a TTL. Previously this engine existed only behind the offline CLI,
	// grounded on the 11-indicator atlas bundle — the API had no grounded
	// chat at all. A failed initial snapshot degrades to not mounting the
	// routes rather than failing the whole server.
	if chatPlugin, err := gateway.NewChatPluginFromStore(ctx, s, 0); err != nil {
		fmt.Printf("  grounded-chat: snapshot unavailable (%v) — /v1/policy/chat not mounted\n", err)
	} else {
		chatPlugin.RegisterRoutes(policyGroup)
		fmt.Println("  grounded-chat: mounted at /v1/policy/chat — live-DB snapshot, 5m TTL")
	}

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

	// Cost cap. /v1/chat is public and unauthenticated, so the only thing
	// bounding model spend is this ledger. See pkg/gateway/chatbudget.go.
	chatCfg, budgetWarnings := gateway.ChatBudgetConfigFromEnv()
	for _, w := range budgetWarnings {
		fmt.Printf("  chat budget: %s\n", w)
	}
	chatBudget := gateway.NewChatBudget(chatCfg)

	r.POST("/v1/chat", newChatProxyHandler(gwTarget, gwToken, &http.Client{Timeout: 3 * time.Minute}, chatBudget))
	authState := "no credential — DOJO_GATEWAY_TOKEN unset, chat returns 503"
	if gwToken != "" {
		authState = "service credential configured (DOJO_GATEWAY_TOKEN)"
	}
	fmt.Printf("  chat:     /v1/chat → %s/v1/chat [%s]\n", gwTarget, authState)
	fmt.Printf("  budget:   $%.2f/day ≈ %d exchanges @ $%.4f · per client $%.4f ≈ %d exchanges · resets 00:00 UTC · in-memory (a restart clears the day's tally)\n",
		chatCfg.DailyBudgetUSD, chatCfg.ExchangesPerDay(), chatCfg.ReferenceExchangeUSD(),
		chatCfg.PerClientDailyUSD(), chatCfg.ExchangesPerClientPerDay())

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
		// /map serves the LISA cluster map (map.html); /explorer serves the
		// county explorer (explorer.html). Two different pages — they must
		// not be merged into aliases of one file again.
		{"/map", "map.html"},
		{"/explorer", "explorer.html"},
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

const (
	// maxChatRequestBytes caps the request body. Purely an out-of-memory
	// guard: the real body is ~25 KB, so this is 80x headroom, and anything
	// approaching it would be refused by the per-client budget long before it
	// arrived here. Without it, io.ReadAll below will happily buffer whatever
	// a caller sends.
	maxChatRequestBytes = 2 << 20 // 2 MiB

	// maxSettleBodyBytes caps how much of the upstream response is buffered in
	// order to read its usage block. Chat replies are single-digit KB;
	// anything larger is streamed straight through and settled at the
	// pre-estimate rather than held in memory.
	maxSettleBodyBytes = 1 << 20 // 1 MiB
)

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
//
// Spend control (added 2026-07-29): every admitted request reserves a
// conservative cost estimate against a daily dollar budget and settles against
// the Gateway's reported usage afterwards. The budget applies to ALL requests,
// including ones carrying an inbound Authorization header. Exempting those
// would be defensible in principle — a request on someone else's credential
// does not spend PDI's money — but the exemption would key off a header the
// caller controls, so anyone holding a leaked DOJO_GATEWAY_TOKEN could present
// it inbound and spend PDI's credit outside the ledger. One rule, no bypass.
func newChatProxyHandler(gwTarget, gwToken string, client *http.Client, budget *gateway.ChatBudget) gin.HandlerFunc {
	if budget == nil {
		// A missing budget is a programming error, not a request to disable
		// the cap. Substitute the default rather than serving uncapped.
		budget = gateway.NewChatBudget(gateway.DefaultChatBudgetConfig())
	}
	cfg := budget.Config()

	return func(c *gin.Context) {
		authz := strings.TrimSpace(c.GetHeader("Authorization"))
		if authz == "" && gwToken != "" {
			authz = "Bearer " + gwToken
		}
		if authz == "" {
			// 503, not 401: the visitor did nothing wrong — the server is
			// missing configuration. Distinguishable from an upstream 401 by
			// both status and the absence of the Gateway's "success" field.
			//
			// Checked before the budget on purpose: a server that cannot call
			// upstream spends nothing, so its budget can never be the reason
			// it is refusing, and answering 429 here would misdirect the
			// operator to the wrong knob.
			c.JSON(http.StatusServiceUnavailable, gateway.ErrorResponse{
				Error:  "chat is not configured on this server",
				Detail: "DOJO_GATEWAY_TOKEN is not set, so PDI holds no credential for the Dojo Gateway. No request was sent upstream. An operator must configure the service credential before chat can answer.",
			})
			return
		}

		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChatRequestBytes)
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				c.JSON(http.StatusRequestEntityTooLarge, gateway.ErrorResponse{
					Error:  "chat request is too large",
					Detail: fmt.Sprintf("The request body exceeds %d bytes. No request was sent upstream.", maxChatRequestBytes),
				})
				return
			}
			c.JSON(http.StatusBadRequest, gateway.ErrorResponse{Error: "read body failed"})
			return
		}

		// Admission: reserve the pessimistic cost. A pre-estimate cannot know
		// how long the answer will run, so it assumes the top of the observed
		// range; settlement below corrects it against the real usage.
		clientKey, _ := gateway.ClientKey(c)
		estimate := cfg.EstimateRequestUSD(len(body))
		reservation, denial := budget.Reserve(clientKey, estimate)
		if denial != nil {
			denial.Abort(c)
			return
		}
		// Catch-all: any exit path that does not settle explicitly (including a
		// panic unwinding into gin.Recovery) books the conservative estimate.
		// Settle is idempotent, so the explicit calls below win.
		defer reservation.Settle(estimate)

		proxyReq, err := http.NewRequestWithContext(c.Request.Context(), "POST",
			gwTarget+"/v1/chat", bytes.NewReader(body))
		if err != nil {
			reservation.Settle(0) // nothing left this process
			c.JSON(http.StatusInternalServerError, gateway.ErrorResponse{Error: "build proxy request failed"})
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.Header.Set("Accept", c.GetHeader("Accept"))
		proxyReq.Header.Set("Authorization", authz)

		resp, err := client.Do(proxyReq)
		if err != nil {
			// No response at all: the Gateway was unreachable, so no model
			// ran. Charging for it would let an outage burn the day's budget
			// and lock chat out for hours after the Gateway came back.
			reservation.Settle(0)
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
		contentType := resp.Header.Get("Content-Type")

		// A stream cannot be buffered without breaking it, and an oversized
		// body must not be held in memory. Both keep the estimate.
		if strings.Contains(strings.ToLower(contentType), "text/event-stream") || resp.ContentLength > maxSettleBodyBytes {
			reservation.Settle(estimate)
			c.DataFromReader(resp.StatusCode, resp.ContentLength, contentType, resp.Body, extraHeaders)
			return
		}

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSettleBodyBytes+1))
		if readErr != nil || len(respBody) > maxSettleBodyBytes {
			reservation.Settle(estimate)
			c.DataFromReader(resp.StatusCode, resp.ContentLength, contentType,
				io.MultiReader(bytes.NewReader(respBody), resp.Body), extraHeaders)
			return
		}

		// Settlement. Real usage beats the guess whenever the Gateway reports
		// it; the fallbacks below are ordered so that an unrecognised shape
		// costs the estimate rather than nothing.
		actual := estimate
		if promptTokens, completionTokens, ok := gateway.UsageFromResponse(respBody); ok {
			actual = cfg.CostUSD(promptTokens, completionTokens)
		} else if resp.StatusCode < 200 || resp.StatusCode > 299 {
			// The Gateway refused — bad request, auth, upstream failure. Those
			// are decided before or instead of generation, so no tokens were
			// billed. A 2xx with no usage block, by contrast, means a model
			// almost certainly did run, and keeps the estimate.
			actual = 0
		}
		reservation.Settle(actual)

		c.DataFromReader(resp.StatusCode, int64(len(respBody)), contentType, bytes.NewReader(respBody), extraHeaders)
	}
}

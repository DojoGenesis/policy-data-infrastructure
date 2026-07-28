// chat-drawer.js — Persistent chat drawer for every PDI page.
// Self-contained JS module. Injects its own CSS and DOM.
// Uses ChatAdapter for context-aware suggestions, messaging, and rich responses.
//
// Usage: add <script src="/static/chat-drawer.js"></script> before </body>
// The drawer auto-initializes on DOMContentLoaded.

(function () {
  'use strict';

  // ── Guard: only init once ──────────────────────────────────────────────────
  if (window.__pdiChatDrawer) return;
  window.__pdiChatDrawer = true;

  // ── Inline CSS ─────────────────────────────────────────────────────────────
  var css = ''
    + '#chat-drawer{position:fixed;bottom:0;left:0;right:0;z-index:100;font-family:var(--font-sans);}'
    + '#chat-drawer *{box-sizing:border-box;}'

    // --- collapsed bar --------------------------------------------------------
    + '.cd-bar{display:flex;align-items:center;gap:10px;padding:8px 16px;'
    + 'background:var(--plane-1);border-top:1px solid var(--border-strong);'
    + 'box-shadow:0 -2px 12px rgba(0,0,0,0.25);cursor:pointer;}'
    + '.cd-bar:hover{border-top-color:var(--accent);}'
    + '.cd-icon{flex-shrink:0;font-size:1.15rem;line-height:1;user-select:none;}'
    + '.cd-input-wrap{flex:1;min-width:0;}'
    + '.cd-input{width:100%;padding:7px 12px;font-size:0.82rem;font-family:inherit;'
    + 'background:var(--surface);color:var(--text);'
    + 'border:1px solid var(--border);border-radius:var(--r-badge);outline:none;'
    + 'transition:border-color 160ms ease;}'
    + '.cd-input:focus{border-color:var(--accent);}'
    + '.cd-input::placeholder{color:var(--subtle);}'
    + '.cd-send-btn{flex-shrink:0;padding:7px 16px;font-size:0.8rem;font-weight:600;font-family:inherit;'
    + 'background:var(--grad-cta);color:var(--bg);border:none;'
    + 'border-radius:var(--r-badge);cursor:pointer;transition:opacity 160ms ease,box-shadow 160ms ease;}'
    + '.cd-send-btn:hover{opacity:0.9;box-shadow:0 0 12px color-mix(in srgb, var(--accent) 30%, transparent);}'
    + '.cd-send-btn:disabled{opacity:0.4;cursor:not-allowed;box-shadow:none;}'
    + '.cd-toggle{flex-shrink:0;padding:4px 8px;font-size:0.75rem;color:var(--muted);'
    + 'background:transparent;border:none;cursor:pointer;font-family:inherit;'
    + 'transition:color 160ms ease;line-height:1;}'
    + '.cd-toggle:hover{color:var(--text);}'

    // --- expanded panel -------------------------------------------------------
    + '.cd-panel{display:none;flex-direction:column;'
    + 'background:var(--plane-1);border-top:1px solid var(--border-strong);'
    + 'max-height:400px;overflow:hidden;transition:max-height 250ms ease;}'
    + '.cd-panel.open{display:flex;}'

    // --- messages area --------------------------------------------------------
    + '.cd-messages{flex:1;overflow-y:auto;padding:12px 16px 8px;'
    + 'display:flex;flex-direction:column;gap:8px;min-height:0;}'
    + '.cd-msg{max-width:85%;padding:8px 12px;border-radius:var(--r-badge);line-height:1.45;font-size:0.82rem;}'
    + '.cd-msg.user{align-self:flex-end;background:var(--accent);color:var(--bg);border-bottom-right-radius:4px;}'
    + '.cd-msg.asst{align-self:flex-start;background:var(--surface);color:var(--text);'
    + 'border:1px solid var(--border);border-bottom-left-radius:4px;}'
    + '.cd-msg.system{align-self:center;background:transparent;color:var(--muted);font-size:0.78rem;text-align:center;max-width:100%;padding:6px 12px;}'
    + '.cd-msg.asst .stat-callout{background:var(--plane-1);}'
    + '.cd-msg.asst .mini-chart{background:var(--plane-1);}'
    + '.cd-msg.asst .data-table thead{background:var(--plane-1);}'

    // --- navigation pills --------------------------------------------------------
    + '.nav-pill{display:inline-block;padding:4px 12px;font-size:0.78rem;font-weight:600;'
    + 'color:var(--accent);text-decoration:none;border:1px solid var(--accent);'
    + 'border-radius:999px;margin:2px 4px 2px 0;transition:all 0.15s ease;white-space:nowrap;}'
    + '.nav-pill:hover{background:var(--accent);color:var(--bg);text-decoration:none;}'

    // --- thinking dots --------------------------------------------------------
    + '.cd-thinking{display:flex;align-items:center;gap:5px;padding:8px 12px;color:var(--muted);font-size:0.78rem;}'
    + '.cd-thinking .cd-dot{width:5px;height:5px;border-radius:50%;background:var(--accent);'
    + 'animation:cd-bounce 1.4s infinite;}'
    + '.cd-thinking .cd-dot:nth-child(2){animation-delay:0.2s;}'
    + '.cd-thinking .cd-dot:nth-child(3){animation-delay:0.4s;}'
    + '@keyframes cd-bounce{0%,80%,100%{opacity:0.3}40%{opacity:1}}'

    // --- suggestions ----------------------------------------------------------
    + '.cd-suggestions{display:flex;flex-wrap:wrap;gap:6px;padding:0 16px 10px;}'
    + '.cd-suggestions button{padding:4px 10px;font-size:0.72rem;font-family:inherit;'
    + 'background:var(--surface);color:var(--muted);'
    + 'border:1px solid var(--border);border-radius:var(--r-chip);cursor:pointer;'
    + 'transition:all 0.15s;white-space:nowrap;}'
    + '.cd-suggestions button:hover{border-color:var(--accent);color:var(--text);}'

    // --- welcome / empty state ------------------------------------------------
    + '.cd-welcome{text-align:center;padding:20px 16px 10px;color:var(--muted);font-size:0.8rem;line-height:1.5;}'
    + '.cd-welcome strong{color:var(--text);}'

    // --- mobile ---------------------------------------------------------------
    + '@media(max-width:600px){'
    + '.cd-panel{max-height:calc(100vh - 48px);}'
    + '.cd-bar{padding:8px 10px;gap:6px;}'
    + '.cd-send-btn{padding:7px 12px;font-size:0.75rem;}'
    + '}';

  var styleEl = document.createElement('style');
  styleEl.textContent = css;
  document.head.appendChild(styleEl);

  // ── Build DOM ──────────────────────────────────────────────────────────────
  var drawer = document.createElement('div');
  drawer.id = 'chat-drawer';
  drawer.innerHTML = ''
    + '<div class="cd-bar">'
    + '  <span class="cd-icon">💬</span>'
    + '  <div class="cd-input-wrap"><input class="cd-input" type="text" placeholder="Ask about this page…" autocomplete="off"></div>'
    + '  <button class="cd-send-btn">Send</button>'
    + '  <button class="cd-toggle" aria-label="Expand chat">▲</button>'
    + '</div>'
    + '<div class="cd-panel">'
    + '  <div class="cd-messages"><div class="cd-welcome"><strong>Ask the Data</strong><br>I can answer about Wisconsin counties, indicators, and policy positions.</div></div>'
    + '  <div class="cd-suggestions"></div>'
    + '</div>';

  document.body.appendChild(drawer);

  // ── Element refs ───────────────────────────────────────────────────────────
  var panel   = drawer.querySelector('.cd-panel');
  var msgs    = drawer.querySelector('.cd-messages');
  var input   = drawer.querySelector('.cd-input');
  var sendBtn = drawer.querySelector('.cd-send-btn');
  var toggle  = drawer.querySelector('.cd-toggle');
  var sugRow  = drawer.querySelector('.cd-suggestions');
  var welcome = drawer.querySelector('.cd-welcome');

  // ── State ──────────────────────────────────────────────────────────────────
  var open = false;
  try { open = localStorage.getItem('pdi-chat-drawer-open') === '1'; } catch (_) {}
  var sending = false;
  var hasMessages = false;

  function persistOpen() {
    try { localStorage.setItem('pdi-chat-drawer-open', open ? '1' : '0'); } catch (_) {}
  }

  function setOpen(v) {
    open = v;
    if (open) {
      panel.classList.add('open');
      toggle.textContent = '▼';
      toggle.setAttribute('aria-label', 'Collapse chat');
    } else {
      panel.classList.remove('open');
      toggle.textContent = '▲';
      toggle.setAttribute('aria-label', 'Expand chat');
    }
    persistOpen();
    if (open) {
      refreshSuggestions();
      setTimeout(function () { msgs.scrollTop = msgs.scrollHeight; }, 50);
    }
  }

  // ── Suggestions ────────────────────────────────────────────────────────────
  function refreshSuggestions() {
    sugRow.innerHTML = '';
    var questions = [];
    if (window.ChatAdapter && typeof ChatAdapter._getSuggestedQuestions === 'function') {
      questions = ChatAdapter._getSuggestedQuestions();
    } else if (window.ChatAdapter && ChatAdapter._placeholders) {
      questions = ChatAdapter._placeholders;
    }
    if (questions.length === 0) return;
    questions.forEach(function (q) {
      var b = document.createElement('button');
      b.textContent = q.length > 80 ? q.slice(0, 77) + '\u2026' : q;
      b.onclick = function () { input.value = q; send(); };
      sugRow.appendChild(b);
    });
  }

  // ── Messages ───────────────────────────────────────────────────────────────
  function addMsg(role, html) {
    if (welcome) { welcome.remove(); welcome = null; }
    hasMessages = true;
    var d = document.createElement('div');
    d.className = 'cd-msg ' + role;
    if (role === 'asst') {
      d.innerHTML = html;
    } else {
      d.textContent = html;
    }
    msgs.appendChild(d);
    msgs.scrollTop = msgs.scrollHeight;
    return d;
  }

  function showThinking(show) {
    var el = msgs.querySelector('.cd-thinking');
    if (show && !el) {
      el = document.createElement('div');
      el.className = 'cd-thinking';
      el.innerHTML = '<span class="cd-dot"></span><span class="cd-dot"></span><span class="cd-dot"></span> Thinking\u2026';
      msgs.appendChild(el);
      msgs.scrollTop = msgs.scrollHeight;
    }
    if (!show && el) el.remove();
  }

  // ── Navigation Command Execution ────────────────────────────────────────────

  // Execute navigation commands extracted from chat responses.
  // Handles: scroll (scroll page to layer), map (update map view), highlight (draw attention to card)
  function executeNavCommands(commands) {
    if (!commands || commands.length === 0) return;

    commands.forEach(function (cmd) {
      switch (cmd.type) {
        case 'scroll':
          // Scroll page to target element (e.g., #layer-3)
          var targetEl = document.getElementById(cmd.target);
          if (targetEl) {
            targetEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
            // Brief highlight flash
            targetEl.style.transition = 'box-shadow 0.3s ease';
            targetEl.style.boxShadow = '0 0 0 3px var(--accent)';
            setTimeout(function () {
              targetEl.style.boxShadow = '';
            }, 2000);
          }
          break;

        case 'map':
          // Dispatch custom event for map page to pick up
          // map.html can listen for 'pdi-map-command' to update indicator/zoom
          var mapEvent = new CustomEvent('pdi-map-command', {
            detail: cmd.params
          });
          document.dispatchEvent(mapEvent);
          break;

        case 'highlight':
          // Highlight evidence card by id (e.g., card-1, evidence-3)
          var cardEl = document.getElementById(cmd.target);
          if (cardEl) {
            cardEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
            cardEl.style.transition = 'box-shadow 0.3s ease, outline 0.3s ease';
            cardEl.style.outline = '3px solid var(--accent)';
            cardEl.style.boxShadow = '0 0 20px color-mix(in srgb, var(--accent) 30%, transparent)';
            setTimeout(function () {
              cardEl.style.outline = '';
              cardEl.style.boxShadow = '';
            }, 3000);
          }
          break;
      }
    });
  }

  // ── Send ───────────────────────────────────────────────────────────────────
  async function send() {
    var text = input.value.trim();
    if (!text || sending) return;
    if (!window.ChatAdapter) {
      addMsg('system', 'Chat service is unavailable right now.');
      return;
    }

    // Auto-open panel if closed
    if (!open) setOpen(true);

    input.value = '';
    sending = true;
    sendBtn.disabled = true;
    addMsg('user', text);
    showThinking(true);

    // Update URL with query for shareability
    if (window.PDIDeepLink) {
      PDIDeepLink.syncURL({ q: encodeURIComponent(text) });
    } else if (window.URLState) {
      URLState.updateParam('q', encodeURIComponent(text));
    }

    var asstEl = null;
    var full = '';
    var first = true;

    try {
      await ChatAdapter.send(text,
        function onChunk(chunk) {
          if (first) { showThinking(false); asstEl = addMsg('asst', ''); first = false; }
          full += chunk;
          if (asstEl) {
            // Show raw text during streaming; parse nav commands after done
            var rawHtml = (window.ChatAdapter && typeof ChatAdapter._parseRichResponse === 'function')
              ? ChatAdapter._parseRichResponse(full)
              : full;
            asstEl.innerHTML = rawHtml.replace(/\n/g, '<br>');
          }
          msgs.scrollTop = msgs.scrollHeight;
        },
        function onDone() {
          // Parse navigation commands from full response and clean display
          if (asstEl && window.ChatAdapter && typeof ChatAdapter._parseNavigationCommands === 'function') {
            var navResult = ChatAdapter._parseNavigationCommands(full);
            var cleanHtml = ChatAdapter._parseRichResponse(navResult.cleanText);
            asstEl.innerHTML = cleanHtml.replace(/\n/g, '<br>');
            // Execute navigation commands (scroll, map, highlight)
            executeNavCommands(navResult.commands);
          }
          sending = false;
          sendBtn.disabled = false;
          input.focus();
          refreshSuggestions();
        }
      );
      if (first) {
        showThinking(false);
        addMsg('system', 'No response received. The chat service may be busy.');
        sending = false;
        sendBtn.disabled = false;
      }
    } catch (e) {
      showThinking(false);
      addMsg('system', 'Error: ' + e.message);
      sending = false;
      sendBtn.disabled = false;
    }
  }

  // ── Event handlers ─────────────────────────────────────────────────────────
  toggle.onclick = function (e) {
    e.stopPropagation();
    setOpen(!open);
  };

  // Click on bar (not on input/button) toggles the panel
  drawer.querySelector('.cd-bar').onclick = function (e) {
    if (e.target === input || e.target === sendBtn || e.target === toggle) return;
    setOpen(!open);
  };

  sendBtn.onclick = function (e) {
    e.stopPropagation();
    send();
  };

  input.onkeydown = function (e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  // Focus input when panel opens
  var panelObserver = new MutationObserver(function () {
    if (open && document.activeElement !== input) {
      // Don't steal focus on load — only when user explicitly opens
    }
  });
  panelObserver.observe(panel, { attributes: true, attributeFilter: ['class'] });

  // ── Page context sync ──────────────────────────────────────────────────────
  // Pages call ChatAdapter.setPageContext() — we refresh suggestions when
  // the page becomes visible (user may have navigated).
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden && open) refreshSuggestions();
  });

  // ── Initial state ──────────────────────────────────────────────────────────
  if (open) setOpen(true);
  refreshSuggestions();

  // ── Public API ─────────────────────────────────────────────────────────────
  window.PDIChatDrawer = {
    open: function () { setOpen(true); },
    close: function () { setOpen(false); },
    toggle: function () { setOpen(!open); },
    refreshSuggestions: refreshSuggestions,
    focus: function () { setOpen(true); setTimeout(function () { input.focus(); }, 300); },
    isOpen: function () { return open; },
    executeNavCommands: executeNavCommands,
    // narrate(command): convenience method that opens the drawer, types the command, and sends it
    narrate: function (command) {
      setOpen(true);
      input.value = command;
      setTimeout(function () { send(); }, 200);
    }
  };

})();
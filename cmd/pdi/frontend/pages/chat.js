// pages/chat.js — Chat interface component wiring to ChatAdapter.
document.addEventListener('alpine:init', () => {
  Alpine.data('chatInterface', () => ({
    messages: [
      {
        id: 'sys-welcome',
        role: 'assistant',
        content: 'Welcome to the Policy Data Infrastructure chat. Ask me about Wisconsin county indicators, census tract data, statistical analyses, or policy positions. Try: "What are the worst counties for food access?" or "Explain the composite disadvantage index."'
      }
    ],
    input: '',
    streaming: false,
    _nextId: 1,

    init() {
      // Check for ?prompt= in the hash (from compare brief or evidence links)
      const hash = window.location.hash || '';
      const promptMatch = hash.match(/[?&]prompt=([^&]+)/);
      if (promptMatch) {
        const prompt = decodeURIComponent(promptMatch[1].replace(/\+/g, ' '));
        // Clean the hash
        window.location.hash = '#/chat';
        // Auto-send after a short delay
        setTimeout(() => {
          this.input = prompt;
          this.send();
        }, 500);
      }
    },

    async send() {
      const text = this.input.trim();
      if (!text || this.streaming) return;

      // Push user message.
      this.messages.push({ id: `u-${this._nextId++}`, role: 'user', content: text });
      this.input = '';

      // Create a placeholder assistant message to stream into.
      const assistantId = `a-${this._nextId++}`;
      this.messages.push({ id: assistantId, role: 'assistant', content: '' });
      this.streaming = true;

      // Scroll to bottom after tick.
      this.$nextTick(() => this._scrollToBottom());

      try {
        // Send the latest user message text to the adapter.
        // The Gateway manages session state via session_id.
        await ChatAdapter.send(
          text,
          (chunk) => {
            const msg = this.messages.find(m => m.id === assistantId);
            if (msg) {
              msg.content += chunk;
              this.$nextTick(() => this._scrollToBottom());
            }
          },
          () => {
            this.streaming = false;
          }
        );
      } catch (err) {
        const msg = this.messages.find(m => m.id === assistantId);
        if (msg) msg.content = `Error: ${err.message}`;
        this.streaming = false;
      }
    },

    handleKey(e) {
      // Submit on Enter; allow Shift+Enter for newlines.
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        this.send();
      }
    },

    clear() {
      this.messages = [this.messages[0]]; // keep welcome message
      this._nextId = 1;
    },

    _scrollToBottom() {
      const el = this.$el.querySelector('.chat-messages');
      if (el) el.scrollTop = el.scrollHeight;
    },

    isUser(msg)      { return msg.role === 'user'; },
    isAssistant(msg) { return msg.role === 'assistant'; }
  }));
});

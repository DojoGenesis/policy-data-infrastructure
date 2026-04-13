package htmlcraft

import (
	"fmt"
	"strings"
)

// Component holds the definition of a Web Component that deliverables can use.
type Component struct {
	Tag      string // e.g. "data-table", "chart-bar", "metric-card"
	JSSource string // inline JavaScript source for the custom element
}

// StandardComponents returns the built-in Web Component set. All components
// read their data from window.__DATA__[data-src] so they integrate directly
// with EmbedData output.
func StandardComponents() []Component {
	return []Component{
		{
			Tag: "data-table",
			JSSource: `
customElements.define('data-table', class extends HTMLElement {
  connectedCallback() {
    const key = this.getAttribute('data-src');
    const rows = (window.__DATA__ || {})[key];
    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      this.innerHTML = '<p class="pdi-empty">No data available.</p>';
      return;
    }
    const cols = Object.keys(rows[0]);
    let sortCol = null, sortAsc = true;
    const render = () => {
      const sorted = sortCol ? [...rows].sort((a, b) => {
        const av = a[sortCol], bv = b[sortCol];
        const cmp = av < bv ? -1 : av > bv ? 1 : 0;
        return sortAsc ? cmp : -cmp;
      }) : rows;
      const thead = cols.map(c => {
        const active = c === sortCol ? (sortAsc ? ' ▲' : ' ▼') : '';
        return '<th data-col="' + c + '" style="cursor:pointer">' + c + active + '</th>';
      }).join('');
      const tbody = sorted.map(row =>
        '<tr>' + cols.map(c => '<td>' + (row[c] !== null && row[c] !== undefined ? row[c] : '') + '</td>').join('') + '</tr>'
      ).join('');
      this.innerHTML = '<div class="pdi-table-wrap"><table class="pdi-table"><thead><tr>' + thead + '</tr></thead><tbody>' + tbody + '</tbody></table></div>';
      this.querySelectorAll('th[data-col]').forEach(th => {
        th.addEventListener('click', () => {
          if (sortCol === th.dataset.col) { sortAsc = !sortAsc; } else { sortCol = th.dataset.col; sortAsc = true; }
          render();
        });
      });
    };
    render();
  }
});`,
		},
		{
			Tag: "chart-bar",
			JSSource: `
customElements.define('chart-bar', class extends HTMLElement {
  connectedCallback() {
    const key = this.getAttribute('data-src');
    const labelField = this.getAttribute('data-label') || 'label';
    const valueField = this.getAttribute('data-value') || 'value';
    const rows = (window.__DATA__ || {})[key];
    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      this.innerHTML = '<p class="pdi-empty">No data available.</p>';
      return;
    }
    const title = this.getAttribute('data-title') || '';
    const canvas = document.createElement('canvas');
    canvas.width = parseInt(this.getAttribute('width') || '600');
    canvas.height = parseInt(this.getAttribute('height') || '300');
    this.innerHTML = title ? '<div class="pdi-chart-title">' + title + '</div>' : '';
    this.appendChild(canvas);
    const ctx = canvas.getContext('2d');
    const labels = rows.map(r => r[labelField]);
    const values = rows.map(r => parseFloat(r[valueField]) || 0);
    const maxVal = Math.max(...values) || 1;
    const barW = Math.floor((canvas.width - 60) / labels.length);
    const chartH = canvas.height - 50;
    ctx.fillStyle = '#f8f9fa';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    values.forEach((v, i) => {
      const barH = Math.round((v / maxVal) * chartH);
      const x = 40 + i * barW + 4;
      const y = chartH - barH + 10;
      ctx.fillStyle = '#1b4a7a';
      ctx.fillRect(x, y, barW - 8, barH);
      ctx.fillStyle = '#333';
      ctx.font = '10px sans-serif';
      ctx.textAlign = 'center';
      const lbl = String(labels[i]);
      ctx.fillText(lbl.length > 10 ? lbl.slice(0, 9) + '…' : lbl, x + (barW - 8) / 2, canvas.height - 8);
    });
    ctx.strokeStyle = '#ccc';
    ctx.beginPath(); ctx.moveTo(38, 10); ctx.lineTo(38, chartH + 10); ctx.lineTo(canvas.width - 10, chartH + 10); ctx.stroke();
  }
});`,
		},
		{
			Tag: "metric-card",
			JSSource: `
customElements.define('metric-card', class extends HTMLElement {
  connectedCallback() {
    const label = this.getAttribute('data-label') || '';
    const value = this.getAttribute('data-value') || '';
    const trend = this.getAttribute('data-trend') || '';
    const unit = this.getAttribute('data-unit') || '';
    const trendIcon = trend === 'up' ? '▲' : trend === 'down' ? '▼' : '';
    const trendClass = trend === 'up' ? 'pdi-trend-up' : trend === 'down' ? 'pdi-trend-down' : '';
    this.innerHTML =
      '<div class="pdi-metric-card">' +
        '<div class="pdi-metric-label">' + label + '</div>' +
        '<div class="pdi-metric-value">' + value + '<span class="pdi-metric-unit">' + unit + '</span></div>' +
        (trendIcon ? '<div class="pdi-metric-trend ' + trendClass + '">' + trendIcon + ' ' + trend + '</div>' : '') +
      '</div>';
  }
});`,
		},
		{
			Tag: "stat-callout",
			JSSource: `
customElements.define('stat-callout', class extends HTMLElement {
  connectedCallback() {
    const number = this.getAttribute('data-number') || '';
    const label = this.getAttribute('data-label') || '';
    const note = this.getAttribute('data-note') || '';
    this.innerHTML =
      '<div class="pdi-stat-callout">' +
        '<div class="pdi-stat-number">' + number + '</div>' +
        '<div class="pdi-stat-label">' + label + '</div>' +
        (note ? '<div class="pdi-stat-note">' + note + '</div>' : '') +
      '</div>';
  }
});`,
		},
	}
}

// InlineComponents generates concatenated <script> blocks for the requested
// component tags. Unknown tags are silently skipped.
func InlineComponents(components []string) string {
	all := StandardComponents()
	index := make(map[string]Component, len(all))
	for _, c := range all {
		index[c.Tag] = c
	}

	var b strings.Builder
	for _, tag := range components {
		c, ok := index[tag]
		if !ok {
			continue
		}
		b.WriteString(fmt.Sprintf("<script>%s</script>\n", c.JSSource))
	}
	return b.String()
}

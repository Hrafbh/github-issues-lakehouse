async function loadCsv(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Cannot load ${path} (${res.status})`);
  const text = await res.text();
  const parsed = Papa.parse(text, { header: true, dynamicTyping: true, skipEmptyLines: true });
  return parsed.data;
}

function colExists(rows, col) {
  return rows.length > 0 && Object.prototype.hasOwnProperty.call(rows[0], col);
}

function toTable(rows, columns, limit = 10) {
  const last = rows.slice(-limit);
  let html = "<table><thead><tr>";
  for (const c of columns) html += `<th>${c}</th>`;
  html += "</tr></thead><tbody>";
  for (const r of last) {
    html += "<tr>";
    for (const c of columns) html += `<td>${r[c] ?? ""}</td>`;
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function plotFlow(rows) {
  const x = rows.map(r => r.month);
  const created = rows.map(r => r.created_count);
  const closed = rows.map(r => r.closed_count);

  const traces = [
    { x, y: created, type: "scatter", mode: "lines+markers", name: "Created" },
    { x, y: closed, type: "scatter", mode: "lines+markers", name: "Closed" },
  ];

  // Optionnel si tu as un bucket dans le CSV
  const shareCol = colExists(rows, "share_closed_within_72h")
    ? "share_closed_within_72h"
    : (colExists(rows, "share_closed_within_168h") ? "share_closed_within_168h" : null);

  if (shareCol) {
    traces.push({
      x, y: rows.map(r => r[shareCol]),
      type: "scatter", mode: "lines+markers",
      name: shareCol, yaxis: "y2"
    });
  }

  const layout = {
    xaxis: { title: "Month" },
    yaxis: { title: "Count" },
    yaxis2: shareCol ? { title: "Share", overlaying: "y", side: "right", tickformat: ".0%" } : undefined,
    legend: { orientation: "h" },
    margin: { t: 20, r: 50, l: 50, b: 50 },
  };

  Plotly.newPlot("chart_flow", traces, layout, { responsive: true });
}

function plotBacklog(rows) {
  const x = rows.map(r => r.month);
  const y = rows.map(r => r.backlog_end);

  Plotly.newPlot("chart_backlog", [{
    x, y, type: "scatter", mode: "lines+markers", name: "Backlog end"
  }], {
    xaxis: { title: "Month" },
    yaxis: { title: "Backlog" },
    margin: { t: 20, r: 30, l: 50, b: 50 },
  }, { responsive: true });
}

function plotResolution(rows) {
  const x = rows.map(r => r.month);
  const traces = [];

  if (colExists(rows, "avg_resolution_hours")) {
    traces.push({ x, y: rows.map(r => r.avg_resolution_hours), type: "scatter", mode: "lines+markers", name: "Avg (h)" });
  }
  if (colExists(rows, "median_resolution_hours")) {
    traces.push({ x, y: rows.map(r => r.median_resolution_hours), type: "scatter", mode: "lines+markers", name: "Median (h)" });
  }
  if (colExists(rows, "p90_resolution_hours")) {
    traces.push({ x, y: rows.map(r => r.p90_resolution_hours), type: "scatter", mode: "lines+markers", name: "P90 (h)" });
  }

  Plotly.newPlot("chart_resolution", traces, {
    xaxis: { title: "Month" },
    yaxis: { title: "Hours" },
    legend: { orientation: "h" },
    margin: { t: 20, r: 30, l: 50, b: 50 },
  }, { responsive: true });
}

async function main() {
  const rows = await loadCsv("data/kpi_monthly_global.csv");

  plotFlow(rows);
  plotBacklog(rows);
  plotResolution(rows);

  const cols = ["month","created_count","closed_count","backlog_end","avg_resolution_hours","p90_resolution_hours"];
  document.getElementById("table_global").innerHTML = toTable(rows, cols, 12);
  document.getElementById("last_update").textContent = `Last update: ${new Date().toISOString().slice(0,10)}`;
}

main().catch(err => {
  document.body.innerHTML = `<pre style="padding:16px;color:#b00">Error: ${err.message}</pre>`;
});

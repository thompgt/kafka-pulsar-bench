/**
 * kpbench Monitoring Dashboard Client
 */

let allRuns = [];
let activeRun = null;
let compareMode = false;
let compareRunA = null;
let compareRunB = null;

const PERCENTILE_KEYS = ['p50', 'p75', 'p90', 'p95', 'p99', 'p99.9'];

// DOM Elements
const elRunSelect = document.getElementById('run-select');
const elBtnRefresh = document.getElementById('btn-refresh');
const elBtnCompareToggle = document.getElementById('btn-compare-toggle');
const elCompareBar = document.getElementById('compare-bar');
const elCompareSelectA = document.getElementById('compare-select-a');
const elCompareSelectB = document.getElementById('compare-select-b');
const elCompareDeltas = document.getElementById('compare-summary-deltas');

// Formatters
function fmtUs(us) {
  if (us === undefined || us === null) return '--';
  if (us >= 1_000_000) return (us / 1_000_000).toFixed(2) + 's';
  if (us >= 1_000) return (us / 1_000).toFixed(2) + 'ms';
  return Math.round(us) + 'µs';
}

function fmtNum(n) {
  if (n === undefined || n === null) return '--';
  return Number(n).toLocaleString();
}

async function fetchRuns() {
  try {
    const res = await fetch('/api/runs');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    allRuns = await res.json();
    populateRunSelectors();
    renderAllRunsTable();

    if (allRuns.length > 0 && !activeRun) {
      loadRun(allRuns[0].run_id);
    }
  } catch (err) {
    console.error('Failed to fetch runs:', err);
    elRunSelect.innerHTML = '<option value="">Failed to load runs</option>';
  }
}

async function loadRun(runId) {
  if (!runId) return;
  try {
    const res = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    activeRun = await res.json();
    elRunSelect.value = runId;
    renderActiveRun();
  } catch (err) {
    console.error(`Failed to load run ${runId}:`, err);
  }
}

function populateRunSelectors() {
  if (!allRuns.length) {
    elRunSelect.innerHTML = '<option value="">No runs found in results/</option>';
    return;
  }

  const optionsHtml = allRuns.map(r => {
    const validPill = r.valid ? '✓' : '✗';
    const rate = r.achieved_rate_hz ? `${fmtNum(Math.round(r.achieved_rate_hz))}/s` : '';
    return `<option value="${r.run_id}">${validPill} [${r.driver.toUpperCase()}] ${r.run_id} (${rate})</option>`;
  }).join('');

  elRunSelect.innerHTML = optionsHtml;
  elCompareSelectA.innerHTML = optionsHtml;
  elCompareSelectB.innerHTML = optionsHtml;

  if (allRuns.length >= 2) {
    elCompareSelectB.selectedIndex = 1;
  }
}

function renderActiveRun() {
  if (!activeRun) return;

  const m = activeRun.metrics || {};
  const cfg = activeRun.config || {};
  const w = cfg.workload || {};
  const t = cfg.topic || {};
  const latency = m.latency || {};
  const resp = (latency.response && latency.response.percentiles_us) || {};
  const svc = (latency.service && latency.service.percentiles_us) || {};
  const lag = (latency.generator_lag && latency.generator_lag.percentiles_us) || {};
  const delivery = m.delivery || {};

  // Banner
  document.getElementById('run-title').textContent = activeRun.run_id;
  document.getElementById('run-description').textContent = cfg.description || 'Executed benchmark run';
  document.getElementById('run-id-display').textContent = activeRun.run_id;

  const elDriver = document.getElementById('badge-driver');
  elDriver.textContent = (activeRun.driver || 'unknown').toUpperCase();
  elDriver.className = `badge badge-${activeRun.driver || 'neutral'}`;

  const elValidity = document.getElementById('badge-validity');
  if (activeRun.valid) {
    elValidity.textContent = 'VALID';
    elValidity.className = 'badge badge-valid';
  } else {
    elValidity.textContent = 'INVALID';
    elValidity.className = 'badge badge-invalid';
    elValidity.title = (activeRun.invalid_reasons || []).join('\n');
  }

  const elWarmup = document.getElementById('badge-warmup');
  if (activeRun.warmup_run) {
    elWarmup.classList.remove('hidden');
  } else {
    elWarmup.classList.add('hidden');
  }

  document.getElementById('meta-workload').textContent = `${w.message_bytes || 0}B @ ${fmtNum(w.target_rate_hz)}/s (${w.duration_s || 0}s)`;
  document.getElementById('meta-partitions').textContent = `${t.partitions || 1}p (durability: ${cfg.producer ? cfg.producer.durability : 'default'})`;
  document.getElementById('meta-timestamp').textContent = m.started_at ? new Date(m.started_at).toLocaleString() : '--';

  // KPIs
  const targetRate = w.target_rate_hz || 0;
  const achievedRate = m.achieved_rate_hz || 0;
  const ratio = m.achieved_rate_ratio ? (m.achieved_rate_ratio * 100).toFixed(1) : '0.0';

  document.getElementById('kpi-achieved-rate').textContent = fmtNum(Math.round(achievedRate));
  document.getElementById('kpi-target-rate').textContent = fmtNum(targetRate);
  document.getElementById('kpi-rate-ratio').textContent = `${ratio}%`;
  document.getElementById('kpi-rate-progress').style.width = `${Math.min(100, Math.max(0, ratio))}%`;

  // Response p50 & p99
  document.getElementById('kpi-p50').textContent = fmtUs(resp.p50);
  document.getElementById('kpi-p50-unit').textContent = '';
  document.getElementById('kpi-service-p50').textContent = `service: ${fmtUs(svc.p50)}`;

  document.getElementById('kpi-p99').textContent = fmtUs(resp.p99);
  document.getElementById('kpi-p99-unit').textContent = '';
  document.getElementById('kpi-p999').textContent = `p99.9: ${fmtUs(resp['p99.9'])}`;

  // Generator Lag
  document.getElementById('kpi-lag').textContent = fmtUs(lag.p50);
  document.getElementById('kpi-lag-unit').textContent = '';
  const elLagStatus = document.getElementById('kpi-lag-status');
  if (lag.p99 > 5000) {
    elLagStatus.textContent = 'Degrading';
    elLagStatus.className = 'badge badge-invalid';
  } else {
    elLagStatus.textContent = 'Clean';
    elLagStatus.className = 'badge badge-valid';
  }

  // Delivery
  const sent = m.sent_measured || m.sent_total || 0;
  const recv = delivery.unique_received || 0;
  const delivPct = sent > 0 ? ((recv / sent) * 100).toFixed(2) : '100.00';
  document.getElementById('kpi-delivery-pct').textContent = `${delivPct}%`;
  document.getElementById('kpi-missing').textContent = fmtNum(delivery.missing || 0);
  document.getElementById('kpi-duplicates').textContent = fmtNum(delivery.duplicates || 0);

  // Render Charts & Tables
  renderPercentilesChart(resp, svc, lag);
  renderPercentilesTable(resp, svc, lag, latency);
  renderThroughputChart(m.throughput_series || []);
  renderEnvironment(activeRun.environment || {}, activeRun.client || {});
}

function renderPercentilesChart(resp, svc, lag) {
  const container = document.getElementById('latency-bars-container');
  if (!container) return;

  const maxVal = Math.max(
    ...PERCENTILE_KEYS.map(k => Math.max(resp[k] || 0, svc[k] || 0, lag[k] || 0)),
    1
  );

  container.innerHTML = PERCENTILE_KEYS.map(p => {
    const rVal = resp[p] || 0;
    const sVal = svc[p] || 0;
    const lVal = lag[p] || 0;

    const rHeight = Math.max(4, (rVal / maxVal) * 100);
    const sHeight = Math.max(4, (sVal / maxVal) * 100);
    const lHeight = Math.max(4, (lVal / maxVal) * 100);

    return `
      <div class="p-bar-group">
        <div class="p-bars-container">
          <div class="p-bar bar-response" style="height: ${rHeight}%" data-tooltip="Response ${p}: ${fmtUs(rVal)}"></div>
          <div class="p-bar bar-service" style="height: ${sHeight}%" data-tooltip="Service ${p}: ${fmtUs(sVal)}"></div>
          <div class="p-bar bar-lag" style="height: ${lHeight}%" data-tooltip="Lag ${p}: ${fmtUs(lVal)}"></div>
        </div>
        <span class="p-label">${p}</span>
      </div>
    `;
  }).join('');
}

function renderPercentilesTable(resp, svc, lag, latency) {
  const tbody = document.getElementById('percentiles-tbody');
  if (!tbody) return;

  const keys = [...PERCENTILE_KEYS, 'max'];
  tbody.innerHTML = keys.map(k => {
    const rVal = k === 'max' ? (latency.response && latency.response.max_us) : resp[k];
    const sVal = k === 'max' ? (latency.service && latency.service.max_us) : svc[k];
    const lVal = k === 'max' ? (latency.generator_lag && latency.generator_lag.max_us) : lag[k];

    return `
      <tr>
        <td class="mono"><strong>${k}</strong></td>
        <td class="mono text-cyan">${fmtUs(rVal)}</td>
        <td class="mono text-violet">${fmtUs(sVal)}</td>
        <td class="mono text-warning">${fmtUs(lVal)}</td>
      </tr>
    `;
  }).join('');
}

function renderThroughputChart(series) {
  const chart = document.getElementById('throughput-chart');
  if (!chart) return;

  if (!series.length) {
    chart.innerHTML = '<div class="text-muted" style="margin:auto">No throughput time series available</div>';
    return;
  }

  const maxMsgs = Math.max(...series.map(s => s.messages || 0), 1);
  chart.innerHTML = series.map(s => {
    const height = Math.max(4, (s.messages / maxMsgs) * 100);
    return `
      <div class="tp-bar-item">
        <div class="tp-bar" style="height: ${height}%" title="Second ${s.second}: ${fmtNum(s.messages)} msgs"></div>
        <span class="p-label">${s.second}s</span>
      </div>
    `;
  }).join('');
}

function renderEnvironment(env, client) {
  const hostList = document.getElementById('env-host-list');
  const dockerList = document.getElementById('env-docker-list');
  const clientList = document.getElementById('env-client-list');
  const gitList = document.getElementById('env-git-list');

  const h = env.host || {};
  const d = env.docker || {};
  const g = env.git || {};

  hostList.innerHTML = `
    <dt>Platform</dt><dd>${h.platform || '--'}</dd>
    <dt>CPU Model</dt><dd>${h.cpu_model || '--'}</dd>
    <dt>CPU Cores</dt><dd>${h.cpu_count || '--'}</dd>
    <dt>RAM</dt><dd>${h.memory_bytes ? (h.memory_bytes / 1024 / 1024 / 1024).toFixed(1) + ' GB' : '--'}</dd>
    <dt>Python</dt><dd>${h.python || '--'} (${h.python_impl || 'CPython'})</dd>
  `;

  dockerList.innerHTML = `
    <dt>Server</dt><dd>${d.server_version || '--'}</dd>
    <dt>CPUs</dt><dd>${d.ncpu || '--'}</dd>
    <dt>Memory</dt><dd>${d.mem_total_bytes ? (d.mem_total_bytes / 1024 / 1024 / 1024).toFixed(1) + ' GB' : '--'}</dd>
  `;

  clientList.innerHTML = Object.entries(client).map(([k, v]) => `
    <dt>${k}</dt><dd>${v}</dd>
  `).join('');

  gitList.innerHTML = `
    <dt>Commit</dt><dd class="text-accent">${g.commit ? g.commit.slice(0, 10) : '--'}</dd>
    <dt>Dirty Tree</dt><dd>${g.dirty === 'true' ? '<span class="text-warning">Yes</span>' : '<span class="text-success">Clean</span>'}</dd>
  `;
}

function renderAllRunsTable(filterDriver = 'all', searchTerm = '') {
  const tbody = document.getElementById('all-runs-tbody');
  if (!tbody) return;

  const filtered = allRuns.filter(r => {
    if (filterDriver !== 'all' && r.driver.toLowerCase() !== filterDriver.toLowerCase()) return false;
    if (searchTerm && !r.run_id.toLowerCase().includes(searchTerm.toLowerCase())) return false;
    return true;
  });

  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="text-muted" style="text-align:center">No matching runs</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(r => `
    <tr>
      <td class="mono"><strong>${r.run_id}</strong></td>
      <td><span class="badge badge-${r.driver}">${r.driver}</span></td>
      <td><span class="badge badge-${r.valid ? 'valid' : 'invalid'}">${r.valid ? 'VALID' : 'INVALID'}</span></td>
      <td class="mono">${r.message_bytes}B, ${r.partitions}p</td>
      <td class="mono">${fmtNum(Math.round(r.achieved_rate_hz))}/s</td>
      <td class="mono text-cyan">${fmtUs(r.p50_us)}</td>
      <td class="mono text-violet">${fmtUs(r.p99_us)}</td>
      <td class="text-muted">${r.started_at ? new Date(r.started_at).toLocaleTimeString() : '--'}</td>
      <td><button class="btn btn-outline" style="padding:2px 8px;font-size:0.75rem" onclick="loadRun('${r.run_id}')">Inspect</button></td>
    </tr>
  `).join('');
}

// Event Listeners
elRunSelect.addEventListener('change', (e) => loadRun(e.target.value));
elBtnRefresh.addEventListener('click', fetchRuns);

elBtnCompareToggle.addEventListener('click', () => {
  compareMode = !compareMode;
  elCompareBar.classList.toggle('hidden', !compareMode);
  elBtnCompareToggle.classList.toggle('btn-primary', compareMode);
  elBtnCompareToggle.classList.toggle('btn-outline', !compareMode);
});

// Tab Switcher
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    const target = document.getElementById(btn.dataset.tab);
    if (target) target.classList.add('active');
  });
});

// Runs Filter Buttons
document.querySelectorAll('.pill-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.pill-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    renderAllRunsTable(btn.dataset.filter, document.getElementById('runs-filter').value);
  });
});

const elSearch = document.getElementById('runs-filter');
if (elSearch) {
  elSearch.addEventListener('input', (e) => {
    const activeFilter = document.querySelector('.pill-btn.active')?.dataset.filter || 'all';
    renderAllRunsTable(activeFilter, e.target.value);
  });
}

// Initial Boot
fetchRuns();

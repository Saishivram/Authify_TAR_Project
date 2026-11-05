/**
 * TAR Protocol - Phase 5 Web Dashboard Frontend
 * Real-time WebSocket telemetry consumption and process control.
 */

let socket = null;
let reconnectTimer = null;
let heartbeatInterval = null;
let currentFilter = 'ALL';
let activeEvents = [];

// -----------------------------------------------------------------------------
// WebSocket Connection & Lifecycle
// -----------------------------------------------------------------------------

function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
  const wsUrl = `${protocol}${window.location.host}/ws`;

  updateWsStatus('connecting', 'Connecting...');

  try {
    socket = new WebSocket(wsUrl);

    socket.onopen = () => {
      updateWsStatus('connected', 'Connected');
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      startHeartbeat();
    };

    socket.onmessage = (event) => {
      try {
        updateWsStatus('connected', 'Connected');
        const data = JSON.parse(event.data);
        if (data.type === 'pong') return;
        updateDashboard(data);
      } catch (err) {
        console.error('Failed parsing WebSocket telemetry:', err);
      }
    };

    socket.onclose = () => {
      stopHeartbeat();
      updateWsStatus('disconnected', 'Reconnecting...');
      scheduleReconnect();
    };

    socket.onerror = () => {
      stopHeartbeat();
      updateWsStatus('disconnected', 'Reconnecting...');
    };
  } catch (err) {
    stopHeartbeat();
    updateWsStatus('disconnected', 'Reconnecting...');
    scheduleReconnect();
  }
}

function startHeartbeat() {
  stopHeartbeat();
  heartbeatInterval = setInterval(() => {
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send('ping');
    }
  }, 3000);
}

function stopHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
}

function scheduleReconnect() {
  if (!reconnectTimer) {
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connectWebSocket();
    }, 1500);
  }
}

function updateWsStatus(state, label) {
  const badge = document.getElementById('ws-status-badge');
  const text = document.getElementById('ws-status-text');
  if (!badge || !text) return;

  badge.className = `ws-status ${state}`;
  text.textContent = label;
}

// -----------------------------------------------------------------------------
// Telemetry Rendering
// -----------------------------------------------------------------------------

function updateDashboard(data) {
  if (!data) return;

  // 1. Component Statuses & Controls
  const comps = data.components || {};
  updateComponentRow('receiver', comps.receiver);
  updateComponentRow('relay', comps.relay);
  updateComponentRow('sender', comps.sender);

  // 2. Delivery Monitoring
  const del = data.delivery || {};
  const sessionStatus = (data.session && data.session.status) || 'IDLE';
  const isIdle = sessionStatus === 'IDLE';

  setText('val-expected', isIdle && !del.expected_records ? '0' : (del.expected_records ?? 0));
  setText('val-delivered', isIdle && !del.delivered_records ? '0' : (del.delivered_records ?? 0));
  setText('val-sent', isIdle && !del.sent_records ? '0' : (del.sent_records ?? 0));
  setText('val-pending', isIdle && !del.pending_records ? '0' : (del.pending_records ?? 0));

  const pct = isIdle && !del.delivered_records ? 0.0 : (del.progress_percent ?? 0.0);
  setText('val-progress-text', `${pct.toFixed(1)}%`);
  const fill = document.getElementById('val-progress-fill');
  if (fill) fill.style.width = `${Math.min(100, Math.max(0, pct))}%`;

  const throughput = isIdle && !del.sent_records ? 0.0 : (del.throughput_records_per_sec ?? 0.0);
  setText('val-throughput', `${throughput.toFixed(1)} rec/s`);
  setText('val-in-flight', isIdle ? 0 : (del.in_flight_records ?? 0));
  setText('val-reorder-buffered', isIdle ? 0 : (del.reorder_buffer_buffered ?? 0));

  // Session State Badge
  const badgeSession = document.getElementById('badge-session-state');
  if (badgeSession) {
    badgeSession.textContent = sessionStatus;
    badgeSession.className = `badge badge-${sessionStatus.toLowerCase()}`;
  }

  // Active Relay Config Summary Sync
  if (data.config) {
    const summary = document.getElementById('active-config-summary');
    if (summary) {
      const l = (data.config.loss_rate * 100).toFixed(1);
      const r = (data.config.reorder_rate * 100).toFixed(1);
      const sDur = data.config.stall_duration_ms;
      const sInt = (data.config.stall_interval_sec).toFixed(1);
      const revL = (data.config.reverse_loss_rate * 100).toFixed(1);
      summary.innerHTML = `<strong>Active Relay:</strong> Loss ${l}% | Reorder ${r}% | Stall ${sDur}ms / ${sInt}s | ACK Loss ${revL}%`;
    }
  }

  // 3. Live Network Statistics
  const net = data.network_stats || {};
  setText('stat-fwd-dropped', net.forward_packets_dropped ?? 0);
  setText('stat-fwd-reordered', net.forward_packets_reordered ?? 0);
  setText('stat-rev-acks', `${net.reverse_packets_sent ?? 0} / ${net.reverse_packets_dropped ?? 0}`);
  setText('stat-retrans', net.retransmissions ?? 0);
  setText('stat-probes', net.probes ?? 0);
  setText('stat-max-gap', `${(net.max_delivery_gap_sec ?? 0.0).toFixed(3)} s`);
  setText('stat-rtt-rto', `${(net.srtt_ms ?? 0.0).toFixed(1)} ms / ${(net.rto_ms ?? 200.0).toFixed(1)} ms`);
  setText('stat-datagram-size', `${net.max_datagram_size_bytes ?? 0} bytes (≤ 1200 MTU)`);

  // 4. Human Attention Alerts
  updateHumanAttention(data.human_attention || []);

  // 5. Event History Stream
  if (data.recent_events) {
    activeEvents = data.recent_events;
    renderEvents();
  }
}

function updateComponentRow(name, comp) {
  if (!comp) return;
  const pill = document.getElementById(`pill-${name}`);
  const btnStart = document.getElementById(`btn-start-${name}`);
  const btnStop = document.getElementById(`btn-stop-${name}`);

  const state = (comp.state || 'STOPPED').toUpperCase();

  if (pill) {
    pill.textContent = state;
    pill.className = `status-pill status-${state.toLowerCase()}`;
  }

  if (btnStart && btnStop) {
    if (state === 'RUNNING') {
      btnStart.disabled = true;
      btnStop.disabled = false;
    } else {
      btnStart.disabled = false;
      btnStop.disabled = true;
    }
  }
}

function updateHumanAttention(alerts) {
  const container = document.getElementById('attention-container');
  if (!container) return;

  if (!alerts || alerts.length === 0) {
    container.className = 'attention-card healthy';
    container.innerHTML = `
      <div class="attention-icon">✓</div>
      <div class="attention-content">
        <div class="attention-title">System Normal</div>
        <div class="attention-desc">All components operating within nominal SLAs. No operator attention required.</div>
      </div>
    `;
    return;
  }

  // Has active alerts
  const highestSev = alerts.some((a) => a.severity === 'error') ? 'error' : 'warning';
  container.className = `attention-card ${highestSev}`;

  let itemsHtml = alerts
    .map(
      (a) => `
      <div class="attention-item">
        <div>
          <strong>${escapeHtml(a.title)}:</strong> ${escapeHtml(a.description)}
          <span style="font-size: 11px; opacity: 0.8; margin-left: 6px;">(${a.timestamp})</span>
        </div>
        <button class="btn btn-sm btn-outline" onclick="dismissAttention('${escapeHtml(a.id)}')">Dismiss</button>
      </div>
    `
    )
    .join('');

  container.innerHTML = `
    <div class="attention-icon">⚠</div>
    <div class="attention-content">
      <div class="attention-title">Attention Required (${alerts.length} active notice${alerts.length > 1 ? 's' : ''})</div>
      <div>${itemsHtml}</div>
    </div>
  `;
}

function renderEvents() {
  const tbody = document.getElementById('log-table-body');
  if (!tbody) return;

  const filtered =
    currentFilter === 'ALL'
      ? activeEvents
      : activeEvents.filter((e) => e.type === currentFilter || e.source === currentFilter);

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty-log-cell">No matching events logged.</td></tr>`;
    return;
  }

  // Reverse so newest appears at top
  const rowsHtml = filtered
    .slice()
    .reverse()
    .map((e) => {
      const srcClass = (e.source || 'sys').toLowerCase();
      const typeClass = (e.type || 'sys').toLowerCase();
      return `
      <tr>
        <td class="log-time">${escapeHtml(e.timestamp)}</td>
        <td><span class="source-pill ${srcClass}">${escapeHtml(e.source)}</span></td>
        <td><span class="type-pill type-${typeClass}">${escapeHtml(e.type)}</span></td>
        <td>${escapeHtml(e.message)}</td>
      </tr>
    `;
    })
    .join('');

  tbody.innerHTML = rowsHtml;
}

function filterEvents(category) {
  currentFilter = category;
  document.querySelectorAll('.filter-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.filter === category);
  });
  renderEvents();
}

// -----------------------------------------------------------------------------
// Interactive Controls & REST API Invocations
// -----------------------------------------------------------------------------

async function startComponent(component) {
  try {
    const res = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ component }),
    });
    const result = await res.json();
    console.log(`Start ${component}:`, result);
  } catch (err) {
    console.error(`Failed to start ${component}:`, err);
  }
}

async function stopComponent(component) {
  try {
    const res = await fetch('/api/stop', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ component }),
    });
    const result = await res.json();
    console.log(`Stop ${component}:`, result);
  } catch (err) {
    console.error(`Failed to stop ${component}:`, err);
  }
}

async function startAllComponents() {
  const countSelect = document.getElementById('select-record-count');
  const windowInput = document.getElementById('input-window');
  const paceInput = document.getElementById('input-pace');

  const record_count = parseInt(countSelect.value, 10) || 2000;
  const send_window = parseInt(windowInput.value, 10) || 200;
  const pace_interval = parseFloat(paceInput.value) || 0.0;

  try {
    const res = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        component: 'all',
        record_count,
        send_window,
        pace_interval,
      }),
    });
    const result = await res.json();
    console.log('Start all result:', result);
  } catch (err) {
    console.error('Failed to start all components:', err);
  }
}

function syncSlider(name) {
  if (name === 'loss') {
    const val = parseFloat(document.getElementById('range-loss').value);
    setText('val-loss', `${val.toFixed(1)}%`);
  } else if (name === 'reorder') {
    const val = parseFloat(document.getElementById('range-reorder').value);
    setText('val-reorder', `${val.toFixed(1)}%`);
  } else if (name === 'stall-duration') {
    const val = parseInt(document.getElementById('range-stall-duration').value, 10);
    setText('val-stall-duration', `${val} ms`);
  } else if (name === 'stall-interval') {
    const val = parseFloat(document.getElementById('range-stall-interval').value);
    setText('val-stall-interval', `${val.toFixed(1)} s`);
  } else if (name === 'reverse-loss') {
    const val = parseFloat(document.getElementById('range-reverse-loss').value);
    setText('val-reverse-loss', `${val.toFixed(1)}%`);
  }
}

async function applyNetworkConfig() {
  const loss = parseFloat(document.getElementById('range-loss').value) / 100.0;
  const reorder = parseFloat(document.getElementById('range-reorder').value) / 100.0;
  const stallDur = parseFloat(document.getElementById('range-stall-duration').value);
  const stallInt = parseFloat(document.getElementById('range-stall-interval').value);
  const revLoss = parseFloat(document.getElementById('range-reverse-loss').value) / 100.0;

  const btn = document.getElementById('btn-apply-config');
  if (btn) btn.textContent = 'Applying...';

  try {
    const res = await fetch('/api/configure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        loss_rate: loss,
        reorder_rate: reorder,
        stall_duration_ms: stallDur,
        stall_interval_sec: stallInt,
        reverse_loss_rate: revLoss,
      }),
    });
    const data = await res.json();

    const summary = document.getElementById('active-config-summary');
    if (summary) {
      summary.innerHTML = `<strong>Active Relay:</strong> Loss ${(loss * 100).toFixed(1)}% | Reorder ${(reorder * 100).toFixed(1)}% | Stall ${stallDur}ms / ${stallInt.toFixed(1)}s | ACK Loss ${(revLoss * 100).toFixed(1)}%`;
    }

    if (btn) {
      btn.textContent = '✓ Applied!';
      setTimeout(() => {
        btn.textContent = 'Apply Network Conditions';
      }, 1500);
    }
  } catch (err) {
    console.error('Failed applying configuration:', err);
    if (btn) btn.textContent = 'Apply Network Conditions';
  }
}

async function clearEventLog() {
  try {
    await fetch('/api/events/clear', { method: 'POST' });
    activeEvents = [];
    renderEvents();
  } catch (err) {
    console.error('Failed clearing events:', err);
  }
}

async function dismissAttention(id) {
  try {
    await fetch('/api/attention/dismiss', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id }),
    });
  } catch (err) {
    console.error('Failed dismissing attention item:', err);
  }
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
  connectWebSocket();
});

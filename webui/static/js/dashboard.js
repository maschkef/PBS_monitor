        // ── Persistent state cache ─────────────────────────────────────────────
        const STATE_CACHE_KEY = 'pbs_monitor_cache_v1';

        const ERROR_DESCRIPTIONS = {
            401: 'Unauthenticated – No valid API token was provided.',
            403: 'Access denied – The API token is invalid or lacks read permissions.',
            404: 'Endpoint not found – The requested API resource does not exist (possibly an outdated URL).',
            429: 'Too many requests – The server rate limit was reached. Please wait a moment and try again.',
            500: 'Internal server error – The server reported an unexpected error.',
            502: 'Bad gateway – The API server is unreachable (proxy error or server restart).',
            503: 'Service unavailable – The server is temporarily overloaded or under maintenance.',
            504: 'Gateway timeout – The API server did not respond in time.',
            no_network: 'No network connection – The server is unreachable (network error, DNS issue, or no internet).',
        };

        let refreshTimer = null;
        let currentDatastores = [];
        let currentPlatformStats = null;
        const backupBrowserCache = new Map();
        const loadingBackupBrowsers = new Set();
        const openBackupBrowsers = new Set();
        const openAlertingPanels = new Set();

        // ── Cache helpers ──────────────────────────────────────────────────────
        function saveStateToCache(datastores, stats) {
            try {
                localStorage.setItem(STATE_CACHE_KEY, JSON.stringify({
                    timestamp: new Date().toISOString(),
                    datastores,
                    stats: stats || null,
                }));
            } catch (_) { /* quota exceeded or unavailable — ignore */ }
        }

        function loadStateFromCache() {
            try {
                const raw = localStorage.getItem(STATE_CACHE_KEY);
                return raw ? JSON.parse(raw) : null;
            } catch (_) { return null; }
        }

        function showCachedBanner(timestamp) {
            document.getElementById('cachedTimestamp').textContent =
                new Date(timestamp).toLocaleString('de-DE');
            document.getElementById('cachedBanner').style.display = '';
        }

        function hideCachedBanner() {
            document.getElementById('cachedBanner').style.display = 'none';
        }

        // ── Error categorisation ───────────────────────────────────────────────
        function categorizeError(message) {
            const m = String(message || '');
            const statusMatch = m.match(/\bHTTP (\d{3})\b/);
            if (statusMatch) {
                const code = parseInt(statusMatch[1]);
                return { code, description: ERROR_DESCRIPTIONS[code] || null };
            }
            if (/failed to fetch|network error|networkerror|api unreachable/i.test(m) ||
                /typeerror/i.test(m)) {
                return { code: 'no_network', description: ERROR_DESCRIPTIONS['no_network'] };
            }
            return { code: null, description: null };
        }

        // ── Blocking error modal (for loadAll failures) ────────────────────────
        function showRefreshError(errorMessage) {
            const { code, description } = categorizeError(errorMessage);
            const descEl = document.getElementById('errorDescriptionBox');
            let html = '';
            if (code !== null) {
                html += `<span class="error-code-badge">${code}</span>`;
            }
            html += description || 'An unknown error occurred.';
            descEl.innerHTML = html;
            document.getElementById('errorRawText').textContent = errorMessage;
            // Auto-expand raw details only when no known description is available
            document.getElementById('errorRawDetails').open = !description;
            document.getElementById('errorModal').style.display = 'flex';
        }

        function retryLoadAll() {
            document.getElementById('errorModal').style.display = 'none';
            loadAll();
        }

        function dismissError() {
            document.getElementById('errorModal').style.display = 'none';
            if (!currentDatastores.length) {
                const cached = loadStateFromCache();
                if (cached && Array.isArray(cached.datastores) && cached.datastores.length) {
                    currentDatastores = cached.datastores;
                    currentPlatformStats = cached.stats || null;
                    renderPlatformStats(currentPlatformStats);
                    renderDatastoreGrid();
                    showCachedBanner(cached.timestamp);
                } else {
                    document.getElementById('content').innerHTML =
                        '<div class="error-msg">No cached data available. Please retry once a connection is established.</div>';
                }
            } else {
                // Live data already visible — just ensure the banner shows the cache timestamp
                const cached = loadStateFromCache();
                if (cached) showCachedBanner(cached.timestamp);
            }
        }

        // ── Non-blocking error strip (for loadLight failures) ─────────────────
        function showRefreshErrorBanner(message) {
            const { code } = categorizeError(message);
            let text = '⚠ Auto-refresh failed';
            if (code === 'no_network') {
                text += ' · No network connection';
            } else if (typeof code === 'number') {
                text += ` · HTTP ${code}`;
            }
            document.getElementById('refreshErrorMsg').textContent = text;
            document.getElementById('refreshErrorBanner').style.display = '';
        }

        function hideRefreshErrorBanner() {
            document.getElementById('refreshErrorBanner').style.display = 'none';
        }

        function retryLoadLight() {
            hideRefreshErrorBanner();
            loadLight();
        }

        function gaugeColor(pct) {
            if (pct >= 90) return 'red';
            if (pct >= 80) return 'yellow';
            return 'green';
        }

        function statusBadge(status) {
            const s = (status || 'unknown').toLowerCase();
            const icons = { ok: '✓', error: '✗', running: '⟳', never: '–' };
            return `<span class="badge ${s}">${icons[s] || '?'} ${escHtml(s)}</span>`;
        }

        function enabledBadge(enabled) {
            return enabled
                ? '<span class="badge enabled">✓ enabled</span>'
                : '<span class="badge disabled">— disabled</span>';
        }

        function alertPriorityLabel(priority) {
            const labels = {
                5: 'urgent',
                4: 'high',
                3: 'default',
                2: 'low',
                1: 'min',
            };
            return labels[priority] || `p${priority}`;
        }

        function scheduleKindLabel(kind) {
            return {
                interval: 'Interval',
                weekly: 'Weekly',
                none: 'None',
            }[kind] || kind || 'None';
        }

        function scheduleToText(schedule) {
            if (!schedule) return '';
            if (schedule.kind === 'interval' && schedule.interval_minutes) {
                return schedule.interval_human || `${schedule.interval_minutes}m`;
            }
            if (schedule.kind === 'daily' && (schedule.slots || []).length) {
                return (schedule.slots || []).map(slot => slot.time).join(', ');
            }
            if (schedule.kind === 'weekly' && (schedule.slots || []).length) {
                return schedule.slots.map(slot => `${slot.weekday_name} ${slot.time}`).join(', ');
            }
            return 'none';
        }

        function scheduleSlotsToInput(schedule) {
            if (!schedule || schedule.kind !== 'weekly') return '';
            return (schedule.slots || []).map(slot => `${slot.weekday_name} ${slot.time}`).join(', ');
        }

        function dailySlotsToInput(schedule) {
            if (!schedule || schedule.kind !== 'daily') return '';
            return (schedule.slots || []).map(slot => slot.time).join(', ');
        }

        function makeGroupDomId(dsId, group) {
            return `${dsId}__${group.namespace || 'root'}__${group.backup_type || 'other'}__${group.backup_id || 'unknown'}`
                .replace(/[^a-zA-Z0-9_-]/g, '_');
        }

        function parseWeeklySlotsInput(text) {
            const weekdayMap = {
                mon: 0,
                tue: 1,
                wed: 2,
                thu: 3,
                fri: 4,
                sat: 5,
                sun: 6,
            };
            if (!text.trim()) return [];
            return text.split(',').map(part => part.trim()).filter(Boolean).map(entry => {
                const match = entry.match(/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d{1,2}):(\d{2})$/i);
                if (!match) {
                    throw new Error(`Invalid weekly slot: ${entry}`);
                }
                const weekday = weekdayMap[match[1].slice(0, 3).toLowerCase()];
                const hour = Number(match[2]);
                const minute = Number(match[3]);
                if (hour > 23 || minute > 59) {
                    throw new Error(`Invalid time in slot: ${entry}`);
                }
                return {
                    weekday,
                    minute_of_day: (hour * 60) + minute,
                };
            });
        }

        function parseDailySlotsInput(text) {
            if (!text.trim()) return [];
            return text.split(',').map(part => part.trim()).filter(Boolean).map(entry => {
                const match = entry.match(/^(\d{1,2}):(\d{2})$/);
                if (!match) {
                    throw new Error(`Invalid daily slot: ${entry}`);
                }
                const hour = Number(match[1]);
                const minute = Number(match[2]);
                if (hour > 23 || minute > 59) {
                    throw new Error(`Invalid time in slot: ${entry}`);
                }
                return { minute_of_day: (hour * 60) + minute };
            });
        }

        function formatDate(iso) {
            if (!iso) return 'N/A';
            return new Date(iso).toLocaleString('de-DE', {
                day: '2-digit', month: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit'
            });
        }

        function formatBytes(value) {
            if (value === null || value === undefined) return 'N/A';
            let size = Number(value);
            if (size === 0) return '0 B';
            const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
            let unitIndex = 0;
            while (Math.abs(size) >= 1024 && unitIndex < units.length - 1) {
                size /= 1024;
                unitIndex += 1;
            }
            return `${size.toFixed(1)} ${units[unitIndex]}`;
        }

        function formatUnixDate(timestamp) {
            if (!timestamp && timestamp !== 0) return 'N/A';
            return formatDate(new Date(timestamp * 1000).toISOString());
        }

        function fetchJson(url) {
            return fetch(url).then(async response => {
                const data = await response.json();
                if (!response.ok) {
                    throw new Error(data.error || data.message || `HTTP ${response.status}`);
                }
                return data;
            });
        }

        function getCsrfToken() {
            return document.querySelector('meta[name="csrf-token"]')?.content || '';
        }

        // Use fetchWrite for all state-changing requests — automatically injects
        // the per-session CSRF token so the server-side require_csrf check passes.
        function fetchWrite(url, options = {}) {
            const headers = { 'X-CSRF-Token': getCsrfToken(), ...(options.headers || {}) };
            return fetch(url, { ...options, headers });
        }

        function protocolLabel(key) {
            return {
                rsync: 'Rsync',
                sftp: 'SFTP',
                zfs_recv: 'ZFS Receive',
            }[key] || key;
        }

        function formatGenericValue(key, value) {
            if (value === null || value === undefined || value === '') return '—';
            if (typeof value === 'number' && /(Bytes|size)$/i.test(key)) {
                return `${formatBytes(value)} (${value})`;
            }
            if (typeof value === 'number' && /(time|creation)$/i.test(key)) {
                return `${formatUnixDate(value)} (${value})`;
            }
            if (Array.isArray(value)) {
                return value.length ? value.join(', ') : '—';
            }
            if (typeof value === 'object') {
                return JSON.stringify(value);
            }
            return value;
        }

        function renderMetaCard(label, value) {
            return `<div class="browser-meta-card">
                <div class="browser-meta-label">${label}</div>
                <div class="browser-meta-value">${escHtml(value) || '—'}</div>
            </div>`;
        }

        function renderProtocolEntry(entry, index) {
            const title = entry.name || entry.filename || entry.fullDataset || `Entry ${index + 1}`;
            const rows = Object.entries(entry).map(([key, value]) => `<div class="status-row">
                <span class="status-label">${escHtml(key)}</span>
                <span class="status-value">${escHtml(String(formatGenericValue(key, value)))}</span>
            </div>`).join('');
            return `<div class="browser-entry">
                <div class="browser-entry-title">${escHtml(title)}</div>
                ${rows}
            </div>`;
        }

        function renderProtocolSection(key, entries) {
            if (!entries) return '';
            const list = Array.isArray(entries) ? entries : [entries];
            if (!list.length) return '';
            return `<div class="browser-section">
                <div class="section-title">${escHtml(protocolLabel(key))}</div>
                ${list.map((entry, index) => renderProtocolEntry(entry, index)).join('')}
            </div>`;
        }

        function renderVerificationPill(state) {
            if (state === 'ok') {
                return '<span class="browser-pill browser-pill--verified">✓ verified</span>';
            }
            if (state === 'failed') {
                return '<span class="browser-pill browser-pill--verify-failed">✗ verify failed</span>';
            }
            // API does not return per-snapshot verification data; show nothing rather than
            // a misleading "unverified" label.
            return '';
        }

        function renderSnapshot(snapshot) {
            return `<details class="browser-snapshot">
                <summary>
                    <div class="browser-snapshot-line">
                        <span class="browser-snapshot-title">${formatUnixDate(snapshot.backup_time)}</span>
                        <div class="browser-pills">
                            <span class="browser-pill">${snapshot.size_human}</span>
                            <span class="browser-pill">${snapshot.file_count} files</span>
                            ${snapshot.protected ? '<span class="browser-pill">protected</span>' : ''}
                            ${renderVerificationPill(snapshot.verification_state)}
                        </div>
                    </div>
                </summary>
                <div class="browser-snapshot-body">
                    ${snapshot.comment ? `<div class="browser-note">${escHtml(snapshot.comment)}</div>` : ''}
                    <table class="browser-file-table">
                        <thead>
                            <tr>
                                <th>File</th>
                                <th>Size</th>
                                <th>Checksum</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${snapshot.files.map(file => `<tr>
                                <td>${escHtml(file.filename)}</td>
                                <td>${file.size_human}</td>
                                <td>${escHtml(file.csum || '—')}</td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </details>`;
        }

        function renderGroup(group) {
            const lastBackup = group.last_backup_iso ? formatDate(group.last_backup_iso) : 'N/A';
            const title = group.display_name || `${group.backup_type}/${group.backup_id}`;
            const subtitle = group.display_name ? `${group.backup_type}/${group.backup_id}` : '';
            return `<details class="browser-group">
                <summary>
                    <div class="browser-group-line">
                        <span class="browser-group-heading">
                            <span class="browser-group-title">${escHtml(title)}</span>
                            ${subtitle ? `<span class="browser-group-subtitle">${escHtml(subtitle)}</span>` : ''}
                        </span>
                        <div class="browser-pills">
                            <span class="browser-pill">${group.snapshot_count} snapshots</span>
                            <span class="browser-pill">last backup ${lastBackup}</span>
                        </div>
                    </div>
                </summary>
                <div class="browser-group-body">
                    <div class="browser-meta-grid">
                        ${group.display_name ? renderMetaCard('Label', group.display_name) : ''}
                        ${renderMetaCard('Type', group.backup_type || '—')}
                        ${renderMetaCard('Backup ID', group.backup_id || '—')}
                        ${renderMetaCard('Last Backup', lastBackup)}
                        ${renderMetaCard('Snapshot Count', String(group.snapshot_count))}
                    </div>
                    ${group.comment ? `<div class="browser-note">${escHtml(group.comment)}</div>` : ''}
                    ${group.snapshots.length
                        ? group.snapshots.map(renderSnapshot).join('')
                        : '<div class="browser-empty">No snapshots in this group.</div>'}
                </div>
            </details>`;
        }

        function renderNamespace(namespace) {
            const shouldOpen = namespace.ns === '';
            const counts = `<div class="browser-pills">
                <span class="browser-pill">${namespace.group_count} groups</span>
                <span class="browser-pill">${namespace.snapshot_count} snapshots</span>
            </div>`;
            return `<details class="browser-namespace" ${shouldOpen ? 'open' : ''}>
                <summary>
                    <div class="browser-summary-line">
                        <span class="browser-namespace-name">${escHtml(namespace.label)}</span>
                        ${counts}
                    </div>
                </summary>
                <div class="browser-namespace-body">
                    ${namespace.comment ? `<div class="browser-note">${escHtml(namespace.comment)}</div>` : ''}
                    ${namespace.error
                        ? `<div class="browser-error">${escHtml(namespace.error)}</div>`
                        : namespace.groups.length
                            ? namespace.groups.map(renderGroup).join('')
                            : '<div class="browser-empty">No backups in this namespace.</div>'}
                </div>
            </details>`;
        }

        function renderBackupBrowser(browser) {
            const summary = browser.summary || {};
            const protocolSections = Object.entries(browser.protocols || {})
                .map(([key, entries]) => renderProtocolSection(key, entries))
                .filter(Boolean)
                .join('');

            return `<div class="browser-shell">
                <div class="browser-summary">
                    <div class="browser-stat">
                        <div class="browser-stat-label">Namespaces</div>
                        <div class="browser-stat-value">${summary.namespace_count || 0}</div>
                    </div>
                    <div class="browser-stat">
                        <div class="browser-stat-label">Groups</div>
                        <div class="browser-stat-value">${summary.group_count || 0}</div>
                    </div>
                    <div class="browser-stat">
                        <div class="browser-stat-label">Snapshots</div>
                        <div class="browser-stat-value">${summary.snapshot_count || 0}</div>
                    </div>
                </div>
                <div class="browser-sections">
                    <div class="browser-section">
                        <div class="section-title">PBS Backup Browser</div>
                        ${browser.namespaces.map(renderNamespace).join('')}
                    </div>
                    ${protocolSections}
                </div>
            </div>`;
        }

        function renderBackupBrowserPanel(dsId) {
            if (!openBackupBrowsers.has(dsId)) {
                return '';
            }

            if (loadingBackupBrowsers.has(dsId)) {
                return `<div class="backup-browser-panel"><div class="browser-shell"><div class="browser-loading">Loading backup browser...</div></div></div>`;
            }

            const payload = backupBrowserCache.get(dsId);
            if (!payload) {
                return `<div class="backup-browser-panel"><div class="browser-shell"><div class="browser-loading">Preparing backup browser...</div></div></div>`;
            }

            if (payload.error) {
                return `<div class="backup-browser-panel"><div class="browser-shell"><div class="browser-error">${escHtml(payload.error)}</div></div></div>`;
            }

            return `<div class="backup-browser-panel">${renderBackupBrowser(payload)}</div>`;
        }

        function renderPlatformStats(data) {
            const el = document.getElementById('platformStats');
            if (!data) { el.innerHTML = ''; return; }

            let html = '';
            if (data.storage) {
                html += `<div class="stat-card">
                    <div class="label">Platform Storage</div>
                    <div class="value">${(data.storage.totalStorage / 1000).toFixed(0)} TB</div>
                    <div class="sub">${data.storage.hosts} hosts online</div>
                </div>`;
            }
            if (data.backups_30d) {
                html += `<div class="stat-card">
                    <div class="label">Backups (30d)</div>
                    <div class="value">${data.backups_30d.count.toLocaleString('de-DE')}</div>
                    <div class="sub">platform-wide</div>
                </div>`;
            }
            if (data.traffic_30d) {
                html += `<div class="stat-card">
                    <div class="label">Traffic (30d)</div>
                    <div class="value">${(data.traffic_30d.total / 1000).toFixed(1)} TB</div>
                    <div class="sub">total transfer</div>
                </div>`;
            }
            el.innerHTML = html;
        }

        function renderRescaleLog(log) {
            if (!log || log.length === 0) return '<div style="color:var(--text-dim);font-size:0.85rem">No resize events</div>';
            return '<div class="rescale-timeline">' + log.map(e => {
                const dir = e.to_gb > e.from_gb ? 'up' : 'down';
                const arrow = dir === 'up' ? '↑' : '↓';
                return `<div class="rescale-entry">
                    <span class="rescale-arrow ${dir}">${arrow}</span>
                    <strong>${e.from_gb}</strong> → <strong>${e.to_gb} GB</strong>
                    <span class="rescale-reason">${escHtml(e.reason || 'unknown')}</span>
                    <span class="rescale-date">${formatDate(e.timestamp)}</span>
                </div>`;
            }).join('') + '</div>';
        }

        function renderAlertItem(alert) {
            return `<div class="alert-item priority-${alert.priority}">
                <div class="alert-head">
                    <span class="alert-title">${escHtml(alert.title)}</span>
                    <span class="alert-priority">${alertPriorityLabel(alert.priority)}</span>
                </div>
                <div class="alert-message">${escHtml(alert.message)}</div>
            </div>`;
        }

        function renderLearnedGroup(group) {
            const slots = (group.slots || []).map(slot =>
                `<span class="browser-pill">${escHtml(slot.weekday_name)} ${escHtml(slot.time)} · ${slot.sample_count}x</span>`
            ).join('');
            return `<div class="learned-group">
                <div class="learned-group-title">${escHtml(group.label)}</div>
                <div class="learned-group-subtitle">namespace ${escHtml(group.namespace)}</div>
                <div class="browser-pills">${slots}</div>
            </div>`;
        }

        function renderGroupRuleEditor(dsId, group) {
            const domId = makeGroupDomId(dsId, group);
            const configured = group.group_rule || {};
            const effective = group.effective_schedule || {};
            const learned = group.learned_schedule || {};
            const effectiveText = scheduleToText(effective);
            const learnedText = scheduleToText(learned);
            const configuredText = scheduleToText(group.configured_schedule || {});
            const scheduleKind = configured.schedule_kind && configured.schedule_kind !== 'none'
                ? configured.schedule_kind
                : (effective.kind && effective.kind !== 'none' ? effective.kind : 'daily');
            const dailyValue = configured.daily_slots && configured.daily_slots.length
                ? configured.daily_slots.map(slot => slot.time).join(', ')
                : dailySlotsToInput(effective);
            const weeklyValue = configured.weekly_slots && configured.weekly_slots.length
                ? configured.weekly_slots.map(slot => `${slot.weekday_name} ${slot.time}`).join(', ')
                : scheduleSlotsToInput(effective);
            const intervalValue = configured.interval_minutes || effective.interval_minutes || '';
            const intervalAnchorValue = configured.interval_anchor_minute != null
                ? `${String(Math.floor(configured.interval_anchor_minute / 60)).padStart(2, '0')}:${String(configured.interval_anchor_minute % 60).padStart(2, '0')}`
                : (effective.interval_anchor_minute != null
                    ? `${String(Math.floor(effective.interval_anchor_minute / 60)).padStart(2, '0')}:${String(effective.interval_anchor_minute % 60).padStart(2, '0')}`
                    : '');
            const timezoneValue = configured.timezone || effective.timezone || 'local';
            const isExpanded = group.group_alert_count > 0;
            const compactSummary = [
                { label: 'Effective', value: effectiveText },
                { label: 'Configured', value: configuredText },
                { label: 'Learned', value: learnedText },
                { label: 'Last Backup', value: formatDate(group.last_backup_at) },
                { label: 'Next Backup', value: group.next_expected_at ? formatDate(group.next_expected_at) : '—' },
            ].map(item => `<div class="learned-group-compact-item"><div class="learned-group-compact-label">${item.label}</div><div class="learned-group-compact-value">${escHtml(item.value)}</div></div>`).join('');

            return `<details class="learned-group" ${isExpanded ? 'open' : ''}>
                <summary>
                    <div class="browser-summary-line">
                        <span class="learned-group-title"><span class="toggle-arrow">&#9658;</span>${escHtml(group.label)}</span>
                        <div class="browser-pills">
                            <span class="browser-pill">namespace ${escHtml(group.namespace)}</span>
                            ${group.group_alert_count ? `<span class="browser-pill">${group.group_alert_count} alerts</span>` : ''}
                            ${group.locked ? '<span class="browser-pill">locked</span>' : '<span class="browser-pill">auto</span>'}
                        </div>
                    </div>
                    <div class="learned-group-compact-grid">${compactSummary}</div>
                </summary>
                <div class="browser-group-body">
                    <div class="soft-note" style="margin-bottom:0.8rem;">When locked, the saved schedule stays authoritative. Learned timing is still shown for comparison, but it is no longer applied automatically.</div>
                    <div class="status-row">
                        <span class="status-label">Lock Configuration</span>
                        <label><input type="checkbox" id="lock-${domId}" ${group.locked ? 'checked' : ''}> lock</label>
                    </div>
                    <div class="status-row">
                        <span class="status-label">Schedule Type</span>
                        <select id="kind-${domId}" class="header-select">
                            <option value="daily" ${scheduleKind === 'daily' ? 'selected' : ''}>daily</option>
                            <option value="weekly" ${scheduleKind === 'weekly' ? 'selected' : ''}>weekly</option>
                            <option value="interval" ${scheduleKind === 'interval' ? 'selected' : ''}>interval</option>
                            <option value="none" ${scheduleKind === 'none' ? 'selected' : ''}>none</option>
                        </select>
                    </div>
                    <div class="status-row">
                        <span class="status-label">Daily Slots</span>
                        <input id="daily-${domId}" class="header-select" style="min-width:220px;" value="${dailyValue}" placeholder="02:00, 14:00">
                    </div>
                    <div class="status-row">
                        <span class="status-label">Weekly Slots</span>
                        <input id="weekly-${domId}" class="header-select" style="min-width:320px;" value="${weeklyValue}" placeholder="Mon 02:00, Tue 02:30">
                    </div>
                    <div class="status-row">
                        <span class="status-label">Interval Minutes</span>
                        <input id="interval-${domId}" class="header-select" style="width:120px;" type="number" min="1" step="1" value="${intervalValue}">
                    </div>
                    <div class="status-row">
                        <span class="status-label">Interval Start (HH:MM)</span>
                        <input id="interval-start-${domId}" class="header-select" style="width:120px;" value="${intervalAnchorValue}" placeholder="06:00" title="Optional start/anchor time for the interval (e.g. 06:00 → backups at 06:00, 08:00, …)">
                    </div>
                    <div class="status-row">
                        <span class="status-label">Timezone</span>
                        <input id="timezone-${domId}" class="header-select" style="min-width:180px;" value="${timezoneValue}">
                    </div>
                    <div style="display:flex;gap:0.6rem;justify-content:flex-end;margin-top:0.9rem;flex-wrap:wrap;">
                        <button class="btn btn-secondary" onclick='applyLearnedToForm("${domId}", ${JSON.stringify(learned).replace(/'/g, '&apos;')})'>Use Learned</button>
                        <button class="btn btn-secondary" onclick='ignoreGroup(${JSON.stringify({
                            datastore_id: group.datastore_id,
                            namespace: group.namespace === 'root' ? '' : group.namespace,
                            backup_type: group.backup_type,
                            backup_id: group.backup_id,
                            display_name: group.label,
                        }).replace(/'/g, '&apos;')})'>Ignore</button>
                        <button class="btn" onclick='saveGroupRule(${JSON.stringify({
                            datastore_id: group.datastore_id,
                            namespace: group.namespace === 'root' ? '' : group.namespace,
                            backup_type: group.backup_type,
                            backup_id: group.backup_id,
                            display_name: group.label,
                            domId,
                        }).replace(/'/g, '&apos;')})'>Save</button>
                    </div>
                </div>
            </details>`;
        }

        function applyLearnedToForm(domId, learned) {
            if (!learned || !learned.kind || learned.kind === 'none') {
                return;
            }
            document.getElementById(`kind-${domId}`).value = learned.kind;
            document.getElementById(`timezone-${domId}`).value = learned.timezone || 'local';
            if (learned.kind === 'interval') {
                document.getElementById(`interval-${domId}`).value = learned.interval_minutes || '';
                document.getElementById(`interval-start-${domId}`).value = '';
                document.getElementById(`daily-${domId}`).value = '';
                document.getElementById(`weekly-${domId}`).value = '';
            } else if (learned.kind === 'daily') {
                document.getElementById(`daily-${domId}`).value = dailySlotsToInput(learned);
                document.getElementById(`interval-${domId}`).value = '';
                document.getElementById(`weekly-${domId}`).value = '';
            } else {
                document.getElementById(`daily-${domId}`).value = '';
                document.getElementById(`weekly-${domId}`).value = scheduleSlotsToInput(learned);
                document.getElementById(`interval-${domId}`).value = '';
            }
        }

        async function saveGroupRule(meta) {
            const kind = document.getElementById(`kind-${meta.domId}`).value;
            const locked = document.getElementById(`lock-${meta.domId}`).checked;
            const timezoneValue = document.getElementById(`timezone-${meta.domId}`).value || 'local';
            const dailyText = document.getElementById(`daily-${meta.domId}`).value;
            const weeklyText = document.getElementById(`weekly-${meta.domId}`).value;
            const intervalValue = document.getElementById(`interval-${meta.domId}`).value;
            const intervalStartText = document.getElementById(`interval-start-${meta.domId}`).value.trim();

            // Parse interval anchor time (HH:MM → minute_of_day)
            let intervalAnchorMinute = null;
            if (kind === 'interval' && intervalStartText) {
                const anchorMatch = intervalStartText.match(/^(\d{1,2}):(\d{2})$/);
                if (!anchorMatch) {
                    window.alert(`Invalid Interval Start time: "${intervalStartText}". Expected HH:MM (e.g. 06:00).`);
                    return;
                }
                const anchorH = Number(anchorMatch[1]);
                const anchorM = Number(anchorMatch[2]);
                if (anchorH > 23 || anchorM > 59) {
                    window.alert(`Invalid Interval Start time: "${intervalStartText}".`);
                    return;
                }
                intervalAnchorMinute = anchorH * 60 + anchorM;
            }

            let dailySlots = [];
            try {
                dailySlots = parseDailySlotsInput(dailyText);
            } catch (error) {
                window.alert(error.message);
                return;
            }

            let weeklySlots = [];
            try {
                weeklySlots = parseWeeklySlotsInput(weeklyText);
            } catch (error) {
                window.alert(error.message);
                return;
            }

            const payload = {
                datastore_id: meta.datastore_id,
                namespace: meta.namespace,
                backup_type: meta.backup_type,
                backup_id: meta.backup_id,
                display_name: meta.display_name,
                locked,
                schedule_kind: kind,
                timezone: timezoneValue,
                daily_slots: dailySlots,
                weekly_slots: weeklySlots,
                interval_minutes: intervalValue ? Number(intervalValue) : null,
                interval_anchor_minute: intervalAnchorMinute,
            };

            const response = await fetchWrite('/api/alerting/group-rule', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await response.json();
            if (!response.ok) {
                window.alert(data.error || 'Failed to save group rule');
                return;
            }
            await loadAll();
        }

        async function ignoreGroup(meta) {
            const confirmed = window.confirm(`Ignore backup group "${meta.display_name}" for alerting and visual schedule checks?`);
            if (!confirmed) {
                return;
            }

            const response = await fetchWrite('/api/alerting/ignore-group', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    datastore_id: meta.datastore_id,
                    namespace: meta.namespace,
                    backup_type: meta.backup_type,
                    backup_id: meta.backup_id,
                    display_name: meta.display_name,
                }),
            });
            const data = await response.json();
            if (!response.ok) {
                window.alert(data.error || 'Failed to ignore group');
                return;
            }
            await loadAll();
        }

        async function unignoreGroup(meta) {
            const label = meta.backup_id ? `${meta.backup_type}/${meta.backup_id}` : meta.backup_type;
            const confirmed = window.confirm(`Re-enable alerting for backup group "${label}"?`);
            if (!confirmed) return;
            const response = await fetchWrite('/api/alerting/unignore-group', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    datastore_id: meta.datastore_id,
                    namespace: meta.namespace,
                    backup_type: meta.backup_type,
                    backup_id: meta.backup_id,
                }),
            });
            const data = await response.json();
            if (!response.ok) {
                window.alert(data.error || 'Failed to unignore group');
                return;
            }
            await loadAll();
        }

        function renderIgnoredGroups(dsId, ignoredGroups) {
            if (!ignoredGroups || !ignoredGroups.length) return '';
            const items = ignoredGroups.map(ig => {
                const techId = `${ig.backup_type || '?'}/${ig.backup_id || '?'}`;
                const niceName = ig.display_name || techId;
                const ns = ig.namespace || 'root';
                const subtitle = ig.display_name ? techId : null;
                return `<div class="learned-group" style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem;">
                    <div>
                        <div class="learned-group-title">${escHtml(niceName)}</div>
                        ${subtitle ? `<div class="learned-group-subtitle">${escHtml(subtitle)}</div>` : ''}
                        <div class="learned-group-subtitle">namespace ${escHtml(ns)}</div>
                    </div>
                    <button class="btn btn-secondary" onclick='unignoreGroup(${JSON.stringify({
                        datastore_id: ig.datastore_id,
                        namespace: ig.namespace || '',
                        backup_type: ig.backup_type,
                        backup_id: ig.backup_id,
                    }).replace(/'/g, "&apos;")})'>Unignore</button>
                </div>`;
            }).join('');
            return `<details style="margin-top:0.6rem;">
                <summary style="cursor:pointer;font-size:0.8rem;color:var(--text-dim);padding:0.4rem 0;list-style:none;display:flex;align-items:center;gap:0.4rem;">
                    <span>&#9658;</span> Ignored Groups (${ignoredGroups.length})
                </summary>
                <div class="learned-group-list" style="margin-top:0.6rem;">${items}</div>
            </details>`;
        }

        function renderAlertingSection(dsId, alerting) {
            const alerts = alerting.alerts || [];
            const schedule = alerting.schedule_learning || {};
            const groups = schedule.groups || [];
            return `<div class="section" style="grid-column: 1 / -1;">
                <div class="section-title">Visual Alerting</div>
                <div class="alert-summary-grid">
                    <div class="alert-summary-card">
                        <div class="alert-summary-label">Active Alerts</div>
                        <div class="alert-summary-value">${alerting.alert_count || 0}</div>
                    </div>
                    <div class="alert-summary-card">
                        <div class="alert-summary-label">Learned Groups</div>
                        <div class="alert-summary-value">${schedule.learned_group_count || 0}</div>
                    </div>
                    <div class="alert-summary-card">
                        <div class="alert-summary-label">Active Slots</div>
                        <div class="alert-summary-value">${schedule.active_slot_count || 0}</div>
                    </div>
                    <div class="alert-summary-card">
                        <div class="alert-summary-label">Alerting State</div>
                        <div class="alert-summary-value">${escHtml(alerting.state_source || 'ephemeral')}</div>
                    </div>
                    <div class="alert-summary-card">
                        <div class="alert-summary-label">Rule Config</div>
                        <div class="alert-summary-value">${escHtml(alerting.rules_source || 'ephemeral')}</div>
                    </div>
                </div>
                <div class="soft-note" style="margin-bottom:0.9rem;">Backup evaluation: ${escHtml(alerting.backup_status || 'inventory unavailable')}</div>
                ${alerting.inventory_error ? `<div class="browser-error" style="margin-bottom:0.9rem;">Backup inventory unavailable: ${escHtml(alerting.inventory_error)}</div>` : ''}
                ${alerts.length
                    ? `<div class="alert-list">${alerts.map(renderAlertItem).join('')}</div>`
                    : '<div class="soft-note" style="margin-bottom:0.9rem;">No active alerts for this datastore.</div>'}
                ${groups.length
                    ? `<div class="learned-group-list">${groups.map(group => renderGroupRuleEditor(dsId, group)).join('')}</div>`
                    : '<div class="soft-note">No backup groups available for schedule configuration yet.</div>'}
                ${renderIgnoredGroups(dsId, alerting.ignored_groups)}
            </div>`;
        }

        function renderDatastore(ds) {
            const m = ds.metrics;
            const pct = m.used_percent;
            const color = gaugeColor(pct);
            const browserLabel = openBackupBrowsers.has(ds.id) ? 'Hide Backups' : 'Browse Backups';
            const alertingLabel = openAlertingPanels.has(ds.id) ? 'Hide Alerting' : 'Show Alerting';
            const alerting = ds.alerting || { alerts: [], schedule_learning: {} };

            const issuesHtml = ds.issues.map(i => {
                const cls = ds.health === 'critical' ? 'critical' : 'warning';
                return `<span class="issue-badge ${cls}">${i}</span>`;
            }).join('');

            return `
            <div class="ds-card health-${ds.health}">
                <div class="ds-header">
                    <div class="ds-header-main">
                        <div class="ds-title">
                            <span class="health-dot ${ds.health}"></span>
                            ${escHtml(ds.name)}
                        </div>
                        <div class="ds-id">${escHtml(ds.id)} · ${ds.size_gb} GB · Created ${formatDate(ds.created_at)}</div>
                    </div>
                    <div class="ds-header-side">
                        <div class="ds-issues">${issuesHtml}</div>
                        <button class="btn btn-secondary" onclick="toggleAlertingPanel(${JSON.stringify(ds.id)})">${alertingLabel}</button>
                        <button class="btn btn-secondary" onclick="toggleBackupBrowser(${JSON.stringify(ds.id)})">${browserLabel}</button>
                    </div>
                </div>
                <div class="ds-body">
                    <div class="ds-grid">
                        <!-- Storage -->
                        <div class="section">
                            <div class="section-title">Storage</div>
                            <div class="gauge-percent ${color}">${pct}%</div>
                            <div class="gauge-container">
                                <div class="gauge-bar-bg">
                                    <div class="gauge-bar-fill ${color}" style="width:${Math.min(pct, 100)}%"></div>
                                </div>
                                <div class="gauge-labels">
                                    <span>${m.used_human} used</span>
                                    <span>${m.available_human} free</span>
                                </div>
                            </div>
                            <div class="status-row">
                                <span class="status-label">Total</span>
                                <span class="status-value">${m.total_human}</span>
                            </div>
                            <div class="status-row">
                                <span class="status-label">Backups</span>
                                <span class="status-value" style="color:var(--cyan);font-weight:700">${m.backup_count}</span>
                            </div>
                        </div>

                        <!-- Jobs -->
                        <div class="section">
                            <div class="section-title">Jobs</div>
                            <div class="status-row">
                                <span class="status-label">GC Status</span>
                                ${statusBadge(ds.gc.status)}
                            </div>
                            <div class="status-row">
                                <span class="status-label">GC Last Run</span>
                                <span class="status-value">${ds.gc.last_run_ago}</span>
                            </div>
                            <div class="status-row">
                                <span class="status-label">GC Next</span>
                                <span class="status-value">${ds.gc.next_in}</span>
                            </div>
                            <div class="status-row" style="margin-top:0.5rem;padding-top:0.5rem;border-top:1px solid rgba(255,255,255,0.06)">
                                <span class="status-label">Verify Status</span>
                                ${statusBadge(ds.verification.status)}
                            </div>
                            <div class="status-row">
                                <span class="status-label">Verify Last Run</span>
                                <span class="status-value">${ds.verification.last_run_ago}</span>
                            </div>
                            <div class="status-row">
                                <span class="status-label">Verify Next</span>
                                <span class="status-value">${ds.verification.next_in}</span>
                            </div>
                        </div>

                        <!-- Retention -->
                        <div class="section">
                            <div class="section-title">Retention (Prune: ${escHtml(ds.prune.schedule)})</div>
                            <div class="prune-grid">
                                <div class="prune-item"><div class="num">${ds.prune.keep_last}</div><div class="lbl">Last</div></div>
                                <div class="prune-item"><div class="num">${ds.prune.keep_hourly}</div><div class="lbl">Hourly</div></div>
                                <div class="prune-item"><div class="num">${ds.prune.keep_daily}</div><div class="lbl">Daily</div></div>
                                <div class="prune-item"><div class="num">${ds.prune.keep_weekly}</div><div class="lbl">Weekly</div></div>
                                <div class="prune-item"><div class="num">${ds.prune.keep_monthly}</div><div class="lbl">Monthly</div></div>
                                <div class="prune-item"><div class="num">${ds.prune.keep_yearly}</div><div class="lbl">Yearly</div></div>
                            </div>
                        </div>

                        <!-- Features -->
                        <div class="section">
                            <div class="section-title">Features</div>
                            <div class="status-row">
                                <span class="status-label">Autoscaling</span>
                                ${enabledBadge(ds.autoscaling.enabled)}
                            </div>
                            ${ds.autoscaling.enabled ? `
                            <div class="status-row">
                                <span class="status-label">Thresholds</span>
                                <span class="status-value">${ds.autoscaling.lower_threshold}% – ${ds.autoscaling.upper_threshold}%</span>
                            </div>
                            <div class="status-row">
                                <span class="status-label">Scale Up Only</span>
                                <span class="status-value">${ds.autoscaling.scale_up_only ? 'Yes' : 'No'}</span>
                            </div>` : ''}
                            <div class="status-row" style="margin-top:0.5rem;padding-top:0.5rem;border-top:1px solid rgba(255,255,255,0.06)">
                                <span class="status-label">Immutable Backups</span>
                                ${enabledBadge(ds.immutable_backup.enabled)}
                            </div>
                            ${ds.immutable_backup.disable_requested ? '<div class="status-row"><span class="status-label">⚠️ Disable requested</span></div>' : ''}
                            <div class="status-row" style="margin-top:0.5rem;padding-top:0.5rem;border-top:1px solid rgba(255,255,255,0.06)">
                                <span class="status-label">Replication</span>
                                ${enabledBadge(ds.replication.enabled)}
                            </div>
                            ${ds.replication.enabled ? `
                            <div class="status-row">
                                <span class="status-label">Factor</span>
                                <span class="status-value">${ds.replication.factor}x</span>
                            </div>` : ''}
                        </div>

                        <!-- Rescale Log -->
                        <div class="section" style="grid-column: 1 / -1;">
                            <div class="section-title">Rescale History (${document.getElementById('rescaleRange').value})</div>
                            ${renderRescaleLog(ds.rescale_log)}
                        </div>

                        ${openAlertingPanels.has(ds.id) ? renderAlertingSection(ds.id, alerting) : ''}
                    </div>
                    ${renderBackupBrowserPanel(ds.id)}
                </div>
            </div>`;
        }

        function renderDatastoreGrid() {
            const content = document.getElementById('content');

            if (!currentDatastores.length) {
                content.innerHTML = `<div class="loading" style="color:var(--text-dim)">No datastores found</div>`;
                return;
            }

            content.innerHTML = '<div class="datastores-grid">' +
                currentDatastores.map(renderDatastore).join('') + '</div>';
        }

        async function toggleBackupBrowser(dsId) {
            if (openBackupBrowsers.has(dsId)) {
                openBackupBrowsers.delete(dsId);
                renderDatastoreGrid();
                return;
            }

            openBackupBrowsers.add(dsId);
            renderDatastoreGrid();

            if (backupBrowserCache.has(dsId) || loadingBackupBrowsers.has(dsId)) {
                return;
            }

            loadingBackupBrowsers.add(dsId);
            renderDatastoreGrid();

            try {
                const payload = await fetchJson(`/api/datastores/${dsId}/backups`);
                backupBrowserCache.set(dsId, payload);
            } catch (error) {
                backupBrowserCache.set(dsId, { error: error.message });
            } finally {
                loadingBackupBrowsers.delete(dsId);
                renderDatastoreGrid();
            }
        }

        function toggleAlertingPanel(dsId) {
            if (openAlertingPanels.has(dsId)) {
                openAlertingPanels.delete(dsId);
            } else {
                openAlertingPanels.add(dsId);
            }
            renderDatastoreGrid();
        }

        async function loadAll() {
            const content = document.getElementById('content');

            // Only show full-screen loader when there is nothing to display yet
            if (!currentDatastores.length) {
                content.innerHTML = `<div class="loading"><div class="spinner"></div>Loading datastores...</div>`;
            }

            try {
                const rescaleRange = document.getElementById('rescaleRange').value;
                const [datastores, stats] = await Promise.all([
                    fetchJson(`/api/datastores?rescale_range=${rescaleRange}`),
                    fetch('/api/platform-stats').then(r => r.json())
                ]);

                currentPlatformStats = stats;
                renderPlatformStats(stats);

                currentDatastores = datastores;
                renderDatastoreGrid();

                saveStateToCache(datastores, stats);
                hideCachedBanner();
                hideRefreshErrorBanner();

                document.getElementById('lastUpdated').textContent =
                    'Updated ' + new Date().toLocaleTimeString('de-DE') + ' (full)';

            } catch (e) {
                showRefreshError(e.message);
            }
        }

        /**
         * Lightweight refresh — only fetches frequently-changing data
         * (metrics, health, GC/verification timestamps, alerting).
         * Skips rescale-log and live backup-inventory calls.
         * Static fields (prune config, autoscaling, rescale-log, platform stats)
         * are kept from the last full load.
         */
        async function loadLight() {
            try {
                const datastores = await fetchJson('/api/datastores/metrics');
                if (currentDatastores && currentDatastores.length) {
                    const byId = Object.fromEntries(datastores.map(d => [d.id, d]));
                    currentDatastores = currentDatastores.map(full => {
                        const light = byId[full.id];
                        if (!light) return full;
                        return {
                            ...full,
                            health: light.health,
                            issues: light.issues,
                            metrics: light.metrics,
                            gc: light.gc,
                            verification: light.verification,
                            replication: { ...full.replication, ...light.replication },
                            alerting: light.alerting,
                        };
                    });
                } else {
                    currentDatastores = datastores;
                }
                renderDatastoreGrid();
                saveStateToCache(currentDatastores, currentPlatformStats);
                hideRefreshErrorBanner();
                document.getElementById('lastUpdated').textContent =
                    'Updated ' + new Date().toLocaleTimeString('de-DE') + ' (light)';
            } catch (e) {
                showRefreshErrorBanner(e.message);
            }
        }

        function getRefreshMs() {
            return parseInt(document.getElementById('refreshInterval').value) * 1000;
        }

        function updateRefreshInterval() {
            if (refreshTimer) {
                clearInterval(refreshTimer);
                refreshTimer = setInterval(loadLight, getRefreshMs());
            }
        }

        // Auto-refresh toggle
        document.getElementById('autoRefresh').addEventListener('change', function() {
            if (this.checked) {
                refreshTimer = setInterval(loadLight, getRefreshMs());
            } else {
                clearInterval(refreshTimer);
                refreshTimer = null;
            }
        });

        // Initial load – show cached state immediately, then attempt live refresh
        (function() {
            const cached = loadStateFromCache();
            if (cached && Array.isArray(cached.datastores) && cached.datastores.length) {
                currentDatastores = cached.datastores;
                currentPlatformStats = cached.stats || null;
                renderPlatformStats(currentPlatformStats);
                renderDatastoreGrid();
                showCachedBanner(cached.timestamp);
            }
        })();
        loadAll();
        initWebuiInfo();

        // ── Modal helpers ──────────────────────────────────────────────────────
        function openModal(id) {
            document.getElementById(id).style.display = 'flex';
        }

        function closeModal(id) {
            document.getElementById(id).style.display = 'none';
        }

        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                ['settingsModal', 'testModal', 'logModal', 'errorModal'].forEach(closeModal);
            }
        });

        function switchTab(panelId, btn) {
            const modal = btn.closest('.modal-box');
            modal.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            modal.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(panelId).classList.add('active');
            btn.classList.add('active');
        }

        // ── WebUI info (read-only flag) ─────────────────────────────────────────
        async function initWebuiInfo() {
            try {
                const info = await fetchJson('/api/webui/info');
                _webuiAlertingPath = info.alerting_path || null;
                _webuiPythonExec = info.python_executable || null;
                _isDocker = info.is_docker || false;
                if (info.read_only) {
                    document.getElementById('readOnlyBadge').style.display = '';
                    document.getElementById('saveConfigBtn').disabled = true;
                    document.getElementById('btnLiveTest').disabled = true;
                    document.getElementById('btnPingTest').disabled = true;
                }
            } catch (_) { /* ignore */ }
        }

        // ── Alerting config ────────────────────────────────────────────────────
        async function openTestModal() {
            openModal('testModal');
            updateTestPrioLabel();
        }

        // ── Notification Log ───────────────────────────────────────────────────
        async function openLogModal() {
            openModal('logModal');
            document.getElementById('logModalCount').textContent = 'Loading…';
            document.getElementById('logModalEmpty').style.display = 'none';
            document.getElementById('logModalError').style.display = 'none';
            document.getElementById('logModalTable').innerHTML = '';
            try {
                const data = await fetchJson('/api/alerting/notification-log');
                renderLogModal(data.entries || []);
            } catch (e) {
                document.getElementById('logModalCount').textContent = '';
                const errEl = document.getElementById('logModalError');
                errEl.textContent = 'Failed to load log: ' + (e.message || e);
                errEl.style.display = '';
            }
        }

        function renderLogModal(entries) {
            const countEl = document.getElementById('logModalCount');
            const tbody = document.getElementById('logModalTable');
            const emptyEl = document.getElementById('logModalEmpty');
            tbody.innerHTML = '';
            if (!entries.length) {
                countEl.textContent = '0 entries';
                emptyEl.style.display = '';
                return;
            }
            countEl.textContent = entries.length + ' entr' + (entries.length === 1 ? 'y' : 'ies') + ' (newest first)';
            emptyEl.style.display = 'none';
            const prioColor = p => p >= 5 ? 'var(--red,#e06c75)' : p >= 4 ? 'var(--yellow,#e5c07b)' : 'var(--text-dim)';
            const srcLabel = s => s === 'webui-test' ? 'test' : s || '—';
            // newest-first
            const sorted = [...entries].reverse();
            for (const e of sorted) {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid var(--border)';
                tr.innerHTML = `
                  <td style="padding:0.3rem 0.5rem;white-space:nowrap;color:var(--text-dim)">${formatDate(e.timestamp)}</td>
                  <td style="padding:0.3rem 0.5rem;white-space:nowrap">${escHtml(srcLabel(e.source))}</td>
                  <td style="padding:0.3rem 0.5rem;color:${prioColor(e.priority)};font-weight:600;white-space:nowrap">${e.priority ?? '—'}</td>
                  <td style="padding:0.3rem 0.5rem">${escHtml(e.title || '—')}</td>
                  <td style="padding:0.3rem 0.5rem;color:var(--text-dim);max-width:28rem;white-space:pre-wrap;word-break:break-word">${escHtml(e.message || '—')}</td>
                  <td style="padding:0.3rem 0.5rem;white-space:nowrap;color:var(--text-dim)">${escHtml(e.datastore_name || '—')}</td>`;
                tbody.appendChild(tr);
            }
        }

        async function clearNotificationLog() {
            if (!confirm('Clear all notification log entries?')) return;
            try {
                const res = await fetchWrite('/api/alerting/notification-log', { method: 'DELETE' });
                const data = await res.json();
                if (data.ok) {
                    renderLogModal([]);
                } else {
                    alert('Failed to clear log: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to clear log: ' + (e.message || e));
            }
        }

        function escHtml(str) {
            return String(str ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        }

        async function loadConfig() {
            try {
                const res = await fetchJson('/api/alerting/config');
                const c = res.config;
                _ntfyUrl = c.ntfy_url || null;
                document.getElementById('cfg-ntfy_url').value = c.ntfy_url || '';
                document.getElementById('cfg-ntfy_topic').value = c.ntfy_topic || '';
                document.getElementById('cfg-ntfy_allow_private').checked = c.ntfy_allow_private_url !== false;
                // Never pre-fill the token field with the sentinel — keep it empty and
                // show a hint instead. The server will preserve the existing token when
                // the field is submitted empty.
                const tokenField = document.getElementById('cfg-ntfy_token');
                const tokenHint = document.getElementById('cfg-ntfy_token_hint');
                tokenField.value = '';
                tokenField.placeholder = c.ntfy_token_set ? '(configured — leave blank to keep, type to replace)' : 'empty = no token';
                if (tokenHint) tokenHint.style.display = c.ntfy_token_set ? '' : 'none';
                document.getElementById('cfg-cooldown').value = c.alert_cooldown_minutes ?? 60;
                document.getElementById('cfg-daemon-interval').value = Math.round((c.daemon_interval_seconds ?? 1800) / 60);

                _notifPriorities = c.notification_priorities || { warning: 4, critical: 5 };
                document.getElementById('cfg-prio-warning').value = _notifPriorities.warning ?? 4;
                document.getElementById('cfg-prio-critical').value = _notifPriorities.critical ?? 5;

                const t = c.thresholds || {};
                document.getElementById('cfg-thr-warn').value = t.storage_warn_percent ?? 80;
                document.getElementById('cfg-thr-crit').value = t.storage_crit_percent ?? 90;
                document.getElementById('cfg-thr-gc').value = t.gc_max_age_hours ?? 36;
                document.getElementById('cfg-thr-verify').value = t.verification_max_age_days ?? 14;

                const qh = c.quiet_hours || {};
                document.getElementById('cfg-qh-enabled').value = qh.enabled ? 'true' : 'false';
                document.getElementById('cfg-qh-start').value = qh.start || '22:00';
                document.getElementById('cfg-qh-end').value = qh.end || '07:00';
                document.getElementById('cfg-qh-prio').value = qh.min_priority ?? 4;

                const sl = c.schedule_learning || {};
                document.getElementById('cfg-sl-enabled').value = sl.enabled === false ? 'false' : 'true';
                document.getElementById('cfg-sl-tz').value = sl.timezone || 'local';
                document.getElementById('cfg-sl-history').value = sl.history_window_days ?? 60;
                document.getElementById('cfg-sl-minocc').value = sl.min_occurrences ?? 2;
                document.getElementById('cfg-sl-tolerance').value = sl.time_tolerance_minutes ?? 30;
                document.getElementById('cfg-sl-grace').value = sl.due_grace_minutes ?? 30;
                document.getElementById('cfg-sl-stale').value = sl.stale_after_days ?? 8;
                document.getElementById('cfg-sl-retention').value = sl.snapshot_retention_count ?? 24;

                if (res.read_only) {
                    document.getElementById('saveConfigBtn').disabled = true;
                    document.querySelectorAll('#settingsModal input, #settingsModal select')
                        .forEach(el => el.disabled = true);
                }

                setSettingsMsg('');
            } catch (e) {
                setSettingsMsg('Error loading config: ' + e.message, 'error');
            }
        }

        // Load config when settings modal opens
        document.getElementById('settingsModal').addEventListener('transitionend', function() {}, false);
        document.querySelector('[onclick="openModal(\'settingsModal\')"]')
            .addEventListener('click', function() { loadConfig(); updateCronEntry(); });

        async function saveConfig() {
            const tokenFieldValue = document.getElementById('cfg-ntfy_token').value;
            const payload = {
                ntfy_url: document.getElementById('cfg-ntfy_url').value.trim(),
                ntfy_topic: document.getElementById('cfg-ntfy_topic').value.trim(),
                ntfy_allow_private_url: document.getElementById('cfg-ntfy_allow_private').checked,
                // Only include ntfy_token when the user has actually typed something;
                // omitting it preserves the previously stored secret on the server.
                ...(tokenFieldValue !== '' ? { ntfy_token: tokenFieldValue } : {}),
                alert_cooldown_minutes: parseInt(document.getElementById('cfg-cooldown').value) || 60,
                daemon_interval_seconds: (parseInt(document.getElementById('cfg-daemon-interval').value) || 30) * 60,
                thresholds: {
                    storage_warn_percent: parseInt(document.getElementById('cfg-thr-warn').value),
                    storage_crit_percent: parseInt(document.getElementById('cfg-thr-crit').value),
                    gc_max_age_hours: parseInt(document.getElementById('cfg-thr-gc').value),
                    verification_max_age_days: parseInt(document.getElementById('cfg-thr-verify').value),
                },
                quiet_hours: {
                    enabled: document.getElementById('cfg-qh-enabled').value === 'true',
                    start: document.getElementById('cfg-qh-start').value.trim(),
                    end: document.getElementById('cfg-qh-end').value.trim(),
                    min_priority: parseInt(document.getElementById('cfg-qh-prio').value),
                },
                notification_priorities: {
                    warning: parseInt(document.getElementById('cfg-prio-warning').value),
                    critical: parseInt(document.getElementById('cfg-prio-critical').value),
                },
                schedule_learning: {
                    enabled: document.getElementById('cfg-sl-enabled').value === 'true',
                    timezone: document.getElementById('cfg-sl-tz').value.trim(),
                    history_window_days: parseInt(document.getElementById('cfg-sl-history').value),
                    min_occurrences: parseInt(document.getElementById('cfg-sl-minocc').value),
                    time_tolerance_minutes: parseInt(document.getElementById('cfg-sl-tolerance').value),
                    due_grace_minutes: parseInt(document.getElementById('cfg-sl-grace').value),
                    stale_after_days: parseInt(document.getElementById('cfg-sl-stale').value),
                    snapshot_retention_count: parseInt(document.getElementById('cfg-sl-retention').value),
                },
            };

            try {
                const res = await fetchWrite('/api/alerting/config', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
                setSettingsMsg('✓ Saved.', 'ok');
            } catch (e) {
                setSettingsMsg('Error: ' + e.message, 'error');
            }
        }

        function setSettingsMsg(text, type) {
            const el = document.getElementById('settingsMsg');
            el.textContent = text;
            el.style.display = text ? '' : 'none';
            el.style.color = type === 'error' ? 'var(--red)' : 'var(--green)';
        }

        function priorityLabel(p) {
            return {1: 'min', 2: 'low', 3: 'default', 4: 'high', 5: 'urgent'}[p] || String(p);
        }

        function updateTestPrioLabel() {
            const el = document.getElementById('test-prio-label');
            if (!el) return;
            const sv = document.getElementById('test-severity')?.value || 'warning';
            const p = (_notifPriorities || {})[sv] ?? (sv === 'warning' ? 4 : 5);
            el.textContent = `→ ntfy priority ${p} (${priorityLabel(p)})`;
        }

        // ── Cron entry builder ─────────────────────────────────────────────────
        let _webuiAlertingPath = null;
        let _webuiPythonExec = null;
        let _ntfyUrl = null;
        let _notifPriorities = null;
        let _isDocker = false;

        function updateCronEntry() {
            if (_isDocker) {
                document.getElementById('docker-notice').style.display = 'block';
                document.getElementById('cron-setup').style.display = 'none';
            } else {
                document.getElementById('docker-notice').style.display = 'none';
                document.getElementById('cron-setup').style.display = 'block';
                
                const schedule = document.getElementById('cron-interval').value;
                const alertDir = _webuiAlertingPath || '/path/to/PBS_monitor/alerting';
                const python = _webuiPythonExec || 'python3';
                const entry = `${schedule} cd ${alertDir} && ${python} monitor.py`;
                document.getElementById('cronEntryText').textContent = entry;
            }
        }

        function copyCronEntry() {
            const text = document.getElementById('cronEntryText').textContent;
            navigator.clipboard.writeText(text).then(() => {
                const btn = event.target;
                const orig = btn.textContent;
                btn.textContent = '✓ Kopiert';
                setTimeout(() => { btn.textContent = orig; }, 1800);
            }).catch(() => {
                // Fallback for older browsers
                const ta = document.createElement('textarea');
                ta.value = text;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                document.body.removeChild(ta);
            });
        }

        // ── Alerting test ──────────────────────────────────────────────────────
        function showTestOutput(label, content) {
            document.getElementById('testOutputLabel').textContent = label;
            document.getElementById('testOutputPre').textContent = content;
            document.getElementById('testOutput').style.display = '';
        }

        function formatDryRunResult(data) {
            if (data.error) return 'Error: ' + data.error;
            let out = `Datastores checked: ${data.datastores_checked}\n`;
            out += `Alerts found: ${data.total_alerts} | Would send: ${data.would_send}\n`;
            out += data.quiet_hours_active ? '\u26a0 Quiet hours active\n' : '';
            out += '\n';
            for (const r of data.results) {
                out += `\u2500\u2500 ${r.datastore} (backup status: ${r.backup_status})\n`;
                if (r.inventory_error) {
                    out += `   \u26a0 Inventory unavailable: ${r.inventory_error}\n`;
                }
                if (!r.alerts.length) {
                    out += '   \u2713 No alerts\n';
                }
                for (const a of r.alerts) {
                    const send = a.would_send ? '\u2192 SEND' : `\u21b7 suppressed (${a.suppressed_by})`;
                    const prio = ['','','','default','high','urgent'][a.priority] || a.priority;
                    out += `   [${prio}] ${a.title}: ${a.message}\n`;
                    out += `   ${send}\n`;
                }
                out += '\n';
            }
            return out.trim();
        }

        async function runDryTest() {
            const btn = document.getElementById('btnDryRun');
            btn.disabled = true;
            btn.textContent = '⏳ Running…';
            showTestOutput('Dry run running…', '');
            try {
                const res = await fetchWrite('/api/alerting/test/dry-run', { method: 'POST' });
                const data = await res.json();
                if (!res.ok) {
                    const { description } = categorizeError(`HTTP ${res.status}`);
                    const detail = description ? `${description}\n\n${data.error || ''}` : (data.error || `HTTP ${res.status}`);
                    showTestOutput('Dry run failed', detail.trim());
                } else {
                    showTestOutput('Dry run result', formatDryRunResult(data));
                }
            } catch (e) {
                const { description } = categorizeError(e.message);
                showTestOutput('Error', description ? `${description}\n\n${e.message}` : e.message);
            } finally {
                btn.disabled = false;
                btn.textContent = '🧪 Dry run';
            }
        }

        async function runLiveTest() {
            // Fetch ntfy URL for the confirm message if not yet cached
            if (_ntfyUrl === null) {
                try {
                    const info = await fetchJson('/api/alerting/config');
                    _ntfyUrl = info.config?.ntfy_url || '';
                } catch (_) { _ntfyUrl = ''; }
            }
            const serverHint = _ntfyUrl ? ` (${_ntfyUrl})` : '';
            if (!confirm(`Run full test? Real ntfy notifications will be sent${serverHint}.`)) return;
            const btn = document.getElementById('btnLiveTest');
            btn.disabled = true;
            btn.textContent = '⏳ Running…';
            showTestOutput('Full test running…', '');
            try {
                const res = await fetchWrite('/api/alerting/test/live', { method: 'POST' });
                const data = await res.json();
                const label = data.ok ? '✓ Test complete' : '✗ Test failed';
                let out = data.output || '';
                if (data.error) {
                    const { description } = categorizeError(data.error);
                    out = 'Error: ' + data.error
                        + (description ? `\n       (${description})` : '')
                        + (out ? '\n\n' + out : '');
                }
                showTestOutput(label, out.trim() || '(no output)');
            } catch (e) {
                const { description } = categorizeError(e.message);
                showTestOutput('Error', description ? `${description}\n\n${e.message}` : e.message);
            } finally {
                btn.disabled = false;
                btn.textContent = '🚀 Full test';
            }
        }

        async function runPingTest() {
            const btn = document.getElementById('btnPingTest');
            const severity = document.getElementById('test-severity')?.value || 'warning';
            btn.disabled = true;
            btn.textContent = '⏳ Sending…';
            showTestOutput('Sending test notification…', '');
            try {
                const res = await fetchWrite('/api/alerting/test/notify', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ severity }),
                });
                const data = await res.json();
                if (data.ok) {
                    showTestOutput('✓ Test notification sent',
                        `Notification delivered to ${data.url}\nPriority: ${data.priority} (${priorityLabel(data.priority)})`);
                } else {
                    const { description } = categorizeError(
                        data.status_code ? `HTTP ${data.status_code}` : (data.error || '')
                    );
                    let out = 'Error: ' + (data.error || 'Unknown error');
                    if (description) out += `\n       (${description})`;
                    showTestOutput('✗ Delivery failed', out);
                }
            } catch (e) {
                const { description } = categorizeError(e.message);
                showTestOutput('Error', description ? `${description}\n\n${e.message}` : e.message);
            } finally {
                btn.disabled = false;
                btn.textContent = '📬 Send test notification';
            }
        }

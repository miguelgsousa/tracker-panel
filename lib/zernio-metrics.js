'use strict';
const { createPanelSecurity } = require('./panel-security');

// Only this fixed origin receives the server credential. Never follow redirects.
const API = 'https://zernio.com/api';
const ANALYTICS = new Set(['instagram', 'facebook', 'tiktok', 'youtube', 'linkedin', 'twitter', 'threads', 'pinterest', 'reddit', 'bluesky', 'googlebusiness', 'snapchat']);
const number = v => typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : null;
const text = v => typeof v === 'string' ? v.slice(0, 10000) : '';
const date = v => typeof v === 'string' && Number.isFinite(Date.parse(v)) ? new Date(v).toISOString() : null;
const safeUrl = v => { try { const u = new URL(v); return ['https:', 'http:'].includes(u.protocol) && !u.username && !u.password ? u.href : null; } catch { return null; } };
const emptyMetrics = () => ({ followers: null, reach: null, views: null, interactions: null, engagementRate: null });
const sum = values => values.length && values.every(v => v !== null) ? values.reduce((a, b) => a + b, 0) : null;
function failure(code, status = 502) { return Object.assign(new Error(code), { code, status }); }
function normalizeAccount(a, scope = 'default') {
    if (!a || typeof a._id !== 'string' || !/^[a-zA-Z0-9_-]{1,128}$/.test(a._id) || !/^[a-z]{1,32}$/.test(a.platform || '') || a.isActive === false || a.needsReconnection === true || a.enabled === false) return null;
    return { id: `zernio:${scope}:${a._id}`, scope, providerId: a._id, provider: a.platform, accountType: a.platform === 'facebook' ? 'PAGE' : 'ZERNIO', username: text(a.username), name: text(a.displayName), followers: number(a.followersCount), following: null, mediaCount: null, status: 'connected', lastSynced: date(a.analyticsLastSyncedAt || a.followersLastUpdated), tokenLifetime: null, connectedAt: date(a.createdAt), expiresAt: null, analyticsSupported: ANALYTICS.has(a.platform) };
}
function normalizePost(p, account, range) {
    const platforms = Array.isArray(p.platforms) ? p.platforms : [];
    const platform = platforms.find(v => (typeof v.accountId === 'object' ? v.accountId?._id : v.accountId) === account.providerId && v.platform === account.provider);
    // Never use cross-platform aggregate analytics for a selected account.
    if (platforms.length && !platform) return null;
    if (!platform && p.platform !== account.provider) return null;
    const timestamp = date(p.publishedAt);
    if (!timestamp || Date.parse(timestamp) / 1000 < range.since || Date.parse(timestamp) / 1000 >= range.until || (p.status && p.status !== 'published')) return null;
    const id = text(platform?.platformPostId || p._id);
    if (!id) return null;
    const pending = platform && ((platform.syncStatus && platform.syncStatus !== 'synced') || platform.errorMessage);
    const m = pending ? {} : (platform ? platform.analytics : p.analytics) || {};
    return { id, accountId: account.id, provider: account.provider, account: account.username || account.name, caption: text(p.content), type: text(p.mediaProductType || p.mediaType).toUpperCase(), date: timestamp.slice(0, 10), timestamp, url: safeUrl(platform?.platformPostUrl || p.platformPostUrl), thumbnail: safeUrl(p.thumbnailUrl), reach: number(m.reach), views: number(m.views), likes: number(m.likes), comments: number(m.comments), saved: number(m.saves), shares: number(m.shares), interactions: sum([number(m.likes), number(m.comments), number(m.shares), number(m.saves)]), engagementRate: number(m.engagementRate), metricsScope: 'post_lifetime', lastSynced: date(m.lastUpdated), partial: Boolean(pending), isAd: p.isAd === true };
}
function createZernioMetrics(env = process.env, options = {}) {
    const fetchImpl = options.fetch || globalThis.fetch;
    const now = options.now || Date.now;
    const timeoutMs = Math.min(options.timeoutMs || 8000, 8000);
    const gate = createPanelSecurity({ ...env, METRICS_ENABLED: 'true' });
    const missing = ['METRICS_USERNAME', 'METRICS_PASSWORD'].filter(k => !env[k]?.trim());
    let keys = {};
    try {
        if (env.ZERNIO_API_KEYS) {
            keys = JSON.parse(env.ZERNIO_API_KEYS);
            if (!keys || Array.isArray(keys) || typeof keys !== 'object' || !Object.keys(keys).length || Object.keys(keys).length > 20 || Object.entries(keys).some(([k,v]) => !/^[a-zA-Z0-9_-]{1,40}$/.test(k) || typeof v !== 'string' || !v.trim() || /[\r\n]/.test(v))) throw new Error();
        } else if (env.ZERNIO_API_KEY?.trim()) keys = { default: env.ZERNIO_API_KEY };
        else missing.push('ZERNIO_API_KEY');
        if (Object.values(keys).some(v => v.length > 8192 || /[\r\n\0]/.test(v))) { keys = {}; missing.push('ZERNIO_API_KEY'); }
    } catch { keys = {}; missing.push('ZERNIO_API_KEYS'); }
    try { const u = new URL(env.PUBLIC_BASE_URL); if (!['http:', 'https:'].includes(u.protocol) || u.origin !== env.PUBLIC_BASE_URL) missing.push('PUBLIC_BASE_URL'); } catch { missing.push('PUBLIC_BASE_URL'); }
    const cache = new Map();
    let registry, registryAt = 0, registryPromise, registryRetry = 0, registryError;
    const inFlight = new Map();
    const scopeCooldown = new Map();
    let closed = false, syncAt = -Infinity, lastError = null;
    const controllers = new Set();
    const context = () => ({ calls: 0, deadline: Date.now() + 45000 });
    async function get(path, params, ctx) {
        if (closed) throw failure('metrics_unavailable', 503);
        if (now() < (scopeCooldown.get(ctx.scope) || 0)) throw failure('zernio_rate_limited');
        if (++ctx.calls > 80 || Date.now() >= ctx.deadline) throw failure('fetch_budget_exceeded');
        const controller = new AbortController(); controllers.add(controller);
        let timer;
        try {
            // Promise deadline also bounds test transports that ignore AbortSignal.
            return await Promise.race([(async () => {
                const r = await fetchImpl(`${API}${path}?${new URLSearchParams(params)}`, { method: 'GET', redirect: 'error', signal: controller.signal, headers: { Authorization: `Bearer ${keys[ctx.scope]}`, Accept: 'application/json' } });
                if (r.status === 429) {
                    const raw = r.headers?.get?.('retry-after');
                    const seconds = raw && /^\d+$/.test(raw) ? Number(raw) : raw ? (Date.parse(raw) - now()) / 1000 : 60;
                    scopeCooldown.set(ctx.scope, now() + Math.min(3600, Math.max(30, Number.isFinite(seconds) ? seconds : 60)) * 1000);
                    throw failure('zernio_rate_limited');
                }
                // Bound body size as well as time. Provider errors/body are never forwarded.
                const reader = r.body?.getReader?.();
                let body;
                if (reader) {
                    const chunks = []; let bytes = 0;
                    try { while (true) { const { value, done } = await reader.read(); if (done) break; bytes += value.byteLength; if (bytes > 4 * 1024 * 1024) throw failure('zernio_response_too_large'); chunks.push(Buffer.from(value)); } } finally { reader.releaseLock(); }
                    body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
                } else body = await r.json();
                if (r.status !== 200) {
                    if (r.status === 400 && body?.code === 'platform_api_error' && body?.platformError?.code === 190) throw failure('zernio_account_reconnection_required');
                    throw failure(r.status === 401 ? 'zernio_authentication_failed' : [402, 403].includes(r.status) ? 'zernio_analytics_unavailable' : 'zernio_request_failed');
                }
                if (!body || typeof body !== 'object' || Array.isArray(body)) throw failure('zernio_invalid_response');
                return body;
            })(), new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(failure('zernio_timeout')); }, Math.min(timeoutMs, Math.max(1, ctx.deadline - Date.now()))); })]);
        } catch (error) {
            const code = ['zernio_account_reconnection_required', 'zernio_authentication_failed', 'zernio_analytics_unavailable', 'zernio_rate_limited', 'zernio_request_failed', 'zernio_response_too_large', 'zernio_invalid_response', 'zernio_timeout'].includes(error.code) ? error.code : 'zernio_request_failed';
            lastError = { code, at: new Date(now()).toISOString() }; throw failure(code);
        } finally { clearTimeout(timer); controller.abort(); controllers.delete(controller); }
    }
    async function pages(path, params, field, limit, ctx) {
        const items = []; let stale = false, lastSync = null;
        for (let page = 1; page <= limit; page++) {
            const data = await get(path, { ...params, page, limit: 100 }, ctx);
            if (!Array.isArray(data[field])) throw failure('zernio_invalid_response');
            if (data.hasAnalyticsAccess === false && field === 'posts') throw failure('zernio_analytics_unavailable');
            items.push(...data[field].slice(0, 100));
            stale ||= (data.overview?.dataStaleness?.staleAccountCount || 0) > 0;
            const sync = date(data.overview?.lastSync); if (sync && (!lastSync || sync < lastSync)) lastSync = sync;
            const p = data.pagination;
            const totalPages = number(p?.pages ?? p?.totalPages);
            const total = number(p?.total);
            const more = totalPages !== null ? page < totalPages : total !== null ? page * 100 < total : data[field].length >= 100;
            if (!more) return { items, truncated: data[field].length > 100, stale, lastSync };
        }
        return { items, truncated: true, stale, lastSync };
    }
    async function accounts(ctx) {
        if (registry && now() - registryAt < 60000) return registry;
        if (registryPromise) return registryPromise;
        if (now() < registryRetry) throw failure(registryError || 'zernio_request_failed');
        registryPromise = (async () => {
            try {
                const all = new Map(), errors = []; let truncated = false;
                for (const scope of Object.keys(keys)) {
                    try {
                        ctx.scope = scope;
                        const result = await pages('/v1/accounts', { status: 'connected', includeOverLimit: 'true' }, 'accounts', 10, ctx);
                        truncated ||= result.truncated;
                        for (const raw of result.items) {
                            const a = normalizeAccount(raw, scope);
                            // Duplicate credentials for the same tenant must not double totals.
                            if (a && !all.has(a.providerId)) all.set(a.providerId, a);
                        }
                    } catch (e) { errors.push({ scope, code: e.code || 'zernio_request_failed' }); }
                }
                cache.clear();
                registry = { accounts: [...all.values()], truncated, partial: errors.length > 0 || truncated, errors };
                registryAt = now(); return registry;
            } catch (e) { registryRetry = now() + 30000; registryError = e.code; throw e; }
            finally { registryPromise = null; }
        })();
        return registryPromise;
    }
    function selection(url, all) {
        const provider = url.searchParams.get('provider') || 'all';
        const account = url.searchParams.get('account') || 'all';
        const days = Number(url.searchParams.get('days') || 30);
        if (![1, 7, 30, 90].includes(days) || (provider !== 'all' && !/^[a-z]{1,32}$/.test(provider))) throw failure('invalid_filters', 400);
        const selected = all.filter(a => (provider === 'all' || a.provider === provider) && (account === 'all' || a.id === account));
        if (account !== 'all' && !selected.length) throw failure('account_not_found', 404);
        const until = Math.floor(now() / 86400000) * 86400;
        return { selected, range: { since: until - days * 86400, until, days, timezone: 'UTC' }, key: `${provider}/${account}/${days}/${until}` };
    }
    async function dashboard(url, force) {
        const ctx = context(), listing = await accounts(ctx);
        const { selected, range, key } = selection(url, listing.accounts);
        const previous = cache.get(key);
        if (!force && previous && now() - previous.at < 60000) return previous.value;
        if (!force && inFlight.has(key)) return inFlight.get(key);
        if (inFlight.size >= 3 || (force && (inFlight.has(key) || now() - syncAt < 30000))) throw failure('sync_rate_limited', 429);
        if (force) syncAt = now();
        const work = Promise.resolve().then(async () => {
        try {
            const errors = [...listing.errors], content = [], accountMetrics = [], accountRanges = [];
            let truncated = listing.truncated || selected.length > 20, stale = false;
            for (const a of selected.slice(0, 20)) {
                ctx.scope = a.scope;
                const metrics = emptyMetrics(); metrics.followers = a.followers;
                let lastSynced = null, accountStale = false;
                if (!a.analyticsSupported) errors.push({ accountId: a.id, code: 'platform_analytics_unsupported' });
                else {
                    const params = { fromDate: new Date(range.since * 1000).toISOString().slice(0, 10), toDate: new Date(range.until * 1000 - 1).toISOString().slice(0, 10) };
                    if (['instagram', 'facebook'].includes(a.provider)) {
                        if (a.provider === 'facebook' && range.days > 89) errors.push({ accountId: a.id, code: 'account_insights_range_unsupported' });
                        else try {
                            const untilDate = new Date(range.until * 1000).toISOString().slice(0, 10);
                            const insights = await get(`/v1/analytics/${a.provider}/${a.provider === 'facebook' ? 'page-insights' : 'account-insights'}`, { ...params, toDate: untilDate, accountId: a.providerId, metricType: 'total_value', metrics: a.provider === 'facebook' ? 'page_media_view,page_post_engagements' : 'reach,views,total_interactions' }, ctx);
                            if (insights.success !== true || insights.accountId !== a.providerId || insights.platform !== a.provider || insights.metricType !== 'total_value' || !insights.metrics) throw failure('zernio_invalid_response');
                            const m = insights.metrics;
                            metrics.reach = a.provider === 'instagram' ? number(m.reach?.total) : null;
                            metrics.views = number((a.provider === 'instagram' ? m.views : m.page_media_view)?.total);
                            metrics.interactions = number((a.provider === 'instagram' ? m.total_interactions : m.page_post_engagements)?.total);
                            if (metrics.views === null || metrics.interactions === null || (a.provider === 'instagram' && metrics.reach === null)) errors.push({ accountId: a.id, code: 'account_insights_incomplete' });
                            accountRanges.push({ accountId: a.id, provider: a.provider, range: { ...range, providerSince: date(insights.dateRange?.since), providerUntil: date(insights.dateRange?.until), metricsScope: 'provider_account_period', note: 'Intervalo informado pelo provedor; limites de buckets podem diferir de eventos UTC.' } });
                        } catch (e) { errors.push({ accountId: a.id, code: e.code || 'zernio_request_failed' }); }
                    } else errors.push({ accountId: a.id, code: 'account_insights_unsupported' });
                    try {
                        const result = await pages('/v1/analytics', { ...params, accountId: a.providerId, platform: a.provider, source: 'all', sortBy: 'date', order: 'desc' }, 'posts', 5, ctx);
                        truncated ||= result.truncated; stale ||= result.stale; accountStale = result.stale; lastSynced = result.lastSync;
                        const posts = [...new Map(result.items.map(p => normalizePost(p, a, range)).filter(Boolean).map(p => [p.id, p])).values()];
                        content.push(...posts);
                        if (posts.some(p => p.partial)) errors.push({ accountId: a.id, code: 'post_analytics_pending' });
                        // These are lifetime post totals, NOT period/account insights.
                        // Post metrics remain on content only; never substitute for account-period KPIs.
                        if (!lastSynced) lastSynced = posts.map(p => p.lastSynced).filter(Boolean).sort()[0] || null;
                    } catch (e) { errors.push({ accountId: a.id, code: e.code || 'zernio_request_failed' }); }
                    if (metrics.followers === null) try {
                        const followers = await get('/v1/accounts/follower-stats', { ...params, accountIds: a.providerId, granularity: 'daily' }, ctx);
                        if (!Array.isArray(followers.accounts)) throw failure('zernio_invalid_response');
                        const entry = followers.accounts.find(v => v._id === a.providerId);
                        if (entry) metrics.followers = number(entry.currentFollowers);
                    } catch (e) { errors.push({ accountId: a.id, code: e.code || 'zernio_request_failed' }); }
                }
                accountMetrics.push({ accountId: a.id, provider: a.provider, metrics, stale: accountStale, lastSynced, refreshAttemptedAt: new Date(now()).toISOString() });
            }
            const partial = truncated || errors.length > 0;
            const metrics = emptyMetrics();
            for (const name of ['followers', 'views', 'interactions']) metrics[name] = truncated || listing.partial ? null : sum(accountMetrics.map(a => a.metrics[name]));
            metrics.reach = selected.length === 1 && !listing.partial ? accountMetrics[0]?.metrics.reach ?? null : null;
            const value = { mode: 'live', connected: listing.accounts.length > 0, provider: 'zernio', source: 'Zernio API', accounts: listing.accounts, selectedAccountIds: selected.map(a => a.id), metrics, accountMetrics, accountRanges, content, series: [], errors, range, lastSynced: accountMetrics.map(a => a.lastSynced).filter(Boolean).sort()[0] || null, partial, stale, truncated, refreshAttemptedAt: new Date(now()).toISOString(), metricsScope: 'provider_account_period', contentMetricsScope: 'post_lifetime', providerDelayHours: 48, message: 'KPIs de conta no intervalo informado pela Zernio, sujeitos aos limites de buckets e atraso do provedor. Posts: métricas vitalícias, não atividade do período. Alcance de múltiplas contas não é somado.' };
            if (closed || registry !== listing) throw failure('connection_changed', 409);
            cache.set(key, { at: now(), value }); if (cache.size > 100) cache.delete(cache.keys().next().value);
            return value;
        } finally { inFlight.delete(key); }
        });
        inFlight.set(key, work);
        return work;
    }
    async function handle(req, res) {
        const url = new URL(req.url, 'http://localhost');
        let path = url.pathname.replace(/^\/api\/metrics/, '');
        if (url.pathname.startsWith('/auth/')) path = '/auth/' + url.pathname.slice(6);
        if (closed) return res.status(503).json({ error: 'metrics_unavailable' });
        if (missing.length) return res.status(503).json({ error: 'metrics_access_not_configured', missingConfiguration: missing });
        if (['/disconnect', '/auth/disconnect', '/auth/start'].includes(path) || path.startsWith('/auth/') && path !== '/auth/login') return res.status(405).json({ error: 'zernio_connection_management_disabled', provider: 'zernio' });
        if (path === '/auth/login' && req.method === 'GET') return res.redirect(302, '/');
        const methods = { '/config': 'GET', '/accounts': 'GET', '/dashboard': 'GET', '/sync': 'POST', '/today-views': 'GET', '/diagnostics': 'GET', '/media-details': 'GET' };
        if (!methods[path]) return res.status(404).json({ error: 'not_found' });
        if (req.method !== methods[path]) return res.status(405).json({ error: 'method_not_allowed' });
        try {
            if (path === '/diagnostics') return res.json({ provider: 'zernio', lastOAuthError: null, lastError, connectionManagement: false });
            if (path === '/media-details') return res.status(422).json({ error: 'media_details_unsupported' });
            if (path === '/dashboard' || path === '/sync') return res.json(await dashboard(url, path === '/sync'));
            const listing = await accounts(context());
            if (path === '/accounts') return res.json({ ...listing, provider: 'zernio' });
            if (path === '/config') {
                const providers = Object.fromEntries([...new Set(['instagram', 'facebook', ...listing.accounts.map(a => a.provider)])].map(p => [p, { configured: true, missingConfiguration: [], oauthSupported: false }]));
                return res.json({ configured: true, provider: 'zernio', source: 'zernio', manageConnectionsUrl: 'https://zernio.com/dashboard', readOnlyConnections: true, partial: listing.partial, errors: listing.errors, providers, connected: listing.accounts.length > 0, accountCount: listing.accounts.length, truncated: listing.truncated, sessionOwnership: 'personal', storageAvailable: true, connectionManagement: false, capabilities: { oauth: false, disconnect: false, todayViews: false, mediaDetails: false }, metricsScope: 'provider_account_period', contentMetricsScope: 'post_lifetime' });
            }
            const { selected } = selection(url, listing.accounts);
            const since = Math.floor(now() / 86400000) * 86400;
            const range = { since, until: Math.floor(now() / 1000), timezone: 'UTC' };
            return res.json({ mode: 'live', provider: 'zernio', source: 'Zernio API', connected: listing.accounts.length > 0, accounts: listing.accounts, selectedAccountIds: selected.map(a => a.id), accountViews: selected.map(a => ({ accountId: a.id, provider: a.provider, views: null, range: null, checkedAt: null, error: 'intraday_views_unavailable' })), metrics: { views: null }, knownSubtotal: null, availableAccounts: 0, selectedAccounts: selected.length, partial: true, truncated: listing.truncated, range: null, requestedRange: range, providerDelayHours: null, metricsScope: 'account_current_day', message: 'A Zernio não fornece visualizações intradiárias verificadas neste adaptador.' });
        } catch (e) { return res.status(e.status || 502).json({ error: e.code || 'zernio_request_failed' }); }
    }
    const middleware = (req, res, next) => {
        const path = new URL(req.url, 'http://localhost').pathname;
        if (!(path === '/api/metrics' || path.startsWith('/api/metrics/') || path === '/auth' || path.startsWith('/auth/'))) return next();
        return gate(req, res, () => handle(req, res));
    };
    middleware.close = async () => { closed = true; cache.clear(); registry = null; for (const c of controllers) c.abort(); };
    return middleware;
}
module.exports = { createZernioMetrics, normalizeAccount, normalizePost };

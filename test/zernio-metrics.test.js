'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const { createMetricsIntegration } = require('../lib/metrics-integration');
const { createPanelSecurity, metricsEnabled } = require('../lib/panel-security');
const { normalizePost } = require('../lib/zernio-metrics');
const ENV = { METRICS_PROVIDER: 'zernio', ZERNIO_API_KEYS: JSON.stringify({ alpha: 'secret-alpha', beta: 'secret-beta' }), METRICS_USERNAME: 'user', METRICS_PASSWORD: 'pass', PUBLIC_BASE_URL: 'https://panel.example' };
const CLOCK = Date.parse('2026-09-24T12:00:00Z');
const AUTH = 'Basic ' + Buffer.from('user:pass').toString('base64');
const raw = (id, platform = 'instagram') => ({ _id: id, platform, isActive: true, username: id, displayName: id, followersCount: 9, accessToken: 'do-not-leak', metadata: { token: 'do-not-leak' } });
const response = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json' } });
function fixture() {
    const calls = [];
    const fetch = async (input, options) => {
        const u = new URL(input); const scope = options.headers.Authorization.endsWith('alpha') ? 'alpha' : 'beta';
        calls.push({ u, options, scope });
        if (u.pathname === '/api/v1/accounts') return response({ accounts: [raw(scope, scope === 'alpha' ? 'instagram' : 'facebook')], pagination: { pages: 1 }, hasAnalyticsAccess: true });
        if (u.pathname === '/api/v1/analytics') return response({ posts: [{ _id: 'p-' + scope, platform: scope === 'alpha' ? 'instagram' : 'facebook', status: 'published', publishedAt: '2026-09-23T10:00:00Z', analytics: { views: 900, reach: 800, likes: 1, comments: 2, shares: 3, saves: 4, lastUpdated: '2026-09-24T10:00:00Z' }, content: 'A post', platformPostUrl: 'javascript:alert(1)' }], pagination: { pages: 1 }, hasAnalyticsAccess: true });
        if (u.pathname === '/api/v1/accounts/follower-stats') return response({ accounts: [{ _id: scope, currentFollowers: 10 }] });
        if (u.pathname.includes('insights')) return response({ success: true, accountId: scope, platform: scope === 'alpha' ? 'instagram' : 'facebook', dateRange: { since: u.searchParams.get('fromDate'), until: u.searchParams.get('toDate') }, metricType: 'total_value', metrics: scope === 'alpha' ? { reach: { total: 25 }, views: { total: 100 }, total_interactions: { total: 30 } } : { page_media_view: { total: 200 }, page_post_engagements: { total: 40 } } });
        throw Error('unmocked ' + u.pathname);
    };
    return { calls, fetch };
}
async function app(t, env = ENV, options = {}, outerGate = false) {
    const server = express();
    if (outerGate) server.use(createPanelSecurity(env));
    const middleware = createMetricsIntegration(env, { now: () => CLOCK, ...options });
    server.use(middleware); server.get('/', (_q, r) => r.json({ legacy: true }));
    const listener = server.listen(0, '127.0.0.1'); await new Promise(resolve => listener.once('listening', resolve));
    t.after(async () => { await middleware.close(); await new Promise(resolve => listener.close(resolve)); });
    const request = async (path, options = {}) => {
        const r = await fetch(`http://127.0.0.1:${listener.address().port}${path}`, { redirect: 'manual', ...options, headers: { authorization: AUTH, ...options.headers } });
        return { status: r.status, body: r.status === 302 ? null : await r.json(), headers: r.headers };
    };
    return request;
}
test('rate limits stop a scope immediately, honor Retry-After and leave healthy scopes usable', async t => {
    const f = fixture(); let clock = CLOCK, limited = 0;
    const req = await app(t, ENV, {now: () => clock, fetch: (url, opts) => {
        if (opts.headers.Authorization.endsWith('alpha') && !new URL(url).pathname.endsWith('/accounts')) {
            limited++; return new Response('{}', {status:429, headers:{'Retry-After':'120'}});
        }
        return f.fetch(url, opts);
    }});
    let d = (await req('/api/metrics/dashboard')).body;
    assert.equal(limited, 1); assert.equal(d.accountMetrics[1].metrics.views, 200);
    clock += 61000; await req('/api/metrics/dashboard?days=7'); assert.equal(limited,1);
    clock += 61000; await req('/api/metrics/dashboard?days=7'); assert.equal(limited,2);
});
test('known Meta authorization error is classified without exposing private provider body', async t => {
    const f=fixture();
    const req=await app(t,ENV,{fetch:(url,opts)=>new URL(url).pathname.includes('insights')?response({code:'platform_api_error',platformError:{code:190,message:'secret-alpha private upstream trace'}},400):f.fetch(url,opts)});
    const d=(await req('/api/metrics/dashboard')).body;
    assert.ok(d.errors.some(e=>e.code==='zernio_account_reconnection_required'));
    assert.doesNotMatch(JSON.stringify(d),/secret-alpha|private upstream trace/);
});
test('cold twelve-account six-scope dashboard fits bounded request budget even with follower fallback', async t => {
    let calls=0; const scopes=Object.fromEntries(Array.from({length:6},(_,i)=>['scope'+i,'fixture'+i]));
    const req=await app(t,{...ENV,ZERNIO_API_KEYS:JSON.stringify(scopes)},{fetch:async (url,opts)=>{
        calls++;const u=new URL(url),scope=opts.headers.Authorization.slice(-1),id=u.searchParams.get('accountId');
        if(u.pathname.endsWith('/accounts'))return response({accounts:[0,1].map(i=>({...raw('a'+scope+i,'facebook'),followersCount:null})),pagination:{pages:1}});
        if(u.pathname.endsWith('/follower-stats'))return response({accounts:[{_id:u.searchParams.get('accountIds'),currentFollowers:4}]});
        if(u.pathname.endsWith('/analytics'))return response({posts:[],pagination:{pages:1}});
        return response({success:true,accountId:id,platform:'facebook',metricType:'total_value',metrics:{page_media_view:{total:2},page_post_engagements:{total:1}}});
    }});
    const d=(await req('/api/metrics/dashboard')).body;assert.equal(d.selectedAccountIds.length,12);assert.equal(d.partial,false);assert.equal(d.metrics.views,24);assert.equal(calls,42);
});
test('multi-scope contract, account insights vs lifetime posts, cache and sync cooldown', async t => {
    const f = fixture(), req = await app(t, ENV, f);
    const config = await req('/api/metrics/config');
    assert.equal(config.body.accountCount, 2); assert.equal(config.body.source, 'zernio'); assert.equal(config.body.readOnlyConnections, true);
    let result = await req('/api/metrics/dashboard?days=7');
    assert.equal(result.status, 200); const d = result.body;
    assert.deepEqual(d.selectedAccountIds, ['zernio:alpha:alpha', 'zernio:beta:beta']);
    assert.equal(d.metrics.views, 300); assert.equal(d.metrics.followers, 18); assert.equal(d.metrics.reach, null); assert.equal(d.metrics.interactions, 70);
    assert.equal(d.content[0].views, 900); assert.equal(d.content[0].url, null); assert.equal(d.partial, false);
    assert.equal(d.accountMetrics[0].metrics.reach, 25); assert.equal(d.accountMetrics[1].metrics.reach, null);
    assert.equal(d.contentMetricsScope, 'post_lifetime'); assert.equal(d.metricsScope, 'provider_account_period');
    assert.equal(d.lastSynced, '2026-09-24T10:00:00.000Z');
    for (const c of f.calls) { assert.equal(c.options.redirect, 'error'); assert.equal(c.u.origin, 'https://zernio.com'); assert.equal(c.options.method, 'GET'); if (c.u.searchParams.has('accountId')) assert.equal(c.u.searchParams.get('accountId'), c.scope); }
    const posts = f.calls.find(c => c.u.pathname === '/api/v1/analytics'); assert.equal(posts.u.searchParams.get('toDate'), '2026-09-23');
    const insights = f.calls.find(c => c.u.pathname.includes('insights')); assert.equal(insights.u.searchParams.get('toDate'), '2026-09-24');
    const count = f.calls.length; await req('/api/metrics/dashboard?days=7'); assert.equal(f.calls.length, count);
    result = await req('/api/metrics/sync?days=7', { method: 'POST', headers: { origin: ENV.PUBLIC_BASE_URL } }); assert.equal(result.status, 200);
    assert.equal((await req('/api/metrics/sync?days=7', { method: 'POST', headers: { origin: ENV.PUBLIC_BASE_URL } })).status, 429);
    assert.doesNotMatch(JSON.stringify(d), /secret-alpha|secret-beta|do-not-leak/);
});
test('standalone and whole-origin auth, CSRF and disabled remote operations', async t => {
    const f = fixture(), req = await app(t, ENV, f, true);
    assert.equal((await req('/', { headers: { authorization: '' } })).status, 401);
    assert.equal((await req('/api/metrics/config', { headers: { authorization: '' } })).status, 401);
    assert.equal((await req('/api/metrics/sync', { method: 'POST' })).status, 403);
    assert.equal((await req('/api/metrics/sync', { method: 'POST', headers: { origin: 'https://evil.example' } })).status, 403);
    for (const path of ['/auth/start', '/api/metrics/auth/start', '/auth/instagram/callback']) assert.equal((await req(path)).status, 405);
    assert.equal((await req('/api/metrics/disconnect?account=all', { method: 'POST', headers: { origin: ENV.PUBLIC_BASE_URL } })).status, 405);
    assert.equal(f.calls.length, 0);
    assert.equal((await req('/auth/login')).status, 302);
    assert.equal((await req('/api/metrics/accounts', { method: 'POST', headers: { origin: ENV.PUBLIC_BASE_URL } })).status, 405);
    assert.equal((await req('/api/metrics/config')).headers.get('cache-control'), 'no-store');
});
test('configuration fail closed and provider selection never silently falls back', async t => {
    for (const env of [{ ...ENV, ZERNIO_API_KEYS: '{}' }, { ...ENV, ZERNIO_API_KEYS: '["secret"]' }, { ...ENV, ZERNIO_API_KEYS: '', ZERNIO_API_KEY: '' }, { ...ENV, METRICS_PASSWORD: '' }, { ...ENV, PUBLIC_BASE_URL: '' }, { ...ENV, METRICS_PROVIDER: 'invalid' }]) {
        const req = await app(t, env, { fetch: () => { throw Error('must not fetch'); } }); assert.equal((await req('/api/metrics/config')).status, 503);
    }
    assert.equal(metricsEnabled({ ZERNIO_API_KEYS: '{}' }), true); assert.equal(metricsEnabled({ ZERNIO_API_KEY: 'x' }), true);
});
test('today is unavailable, membership and filters enforced without arbitrary provider lookup', async t => {
    const f = fixture(), req = await app(t, ENV, f);
    assert.equal((await req('/api/metrics/dashboard?account=zernio:evil:other')).status, 404);
    assert.equal((await req('/api/metrics/dashboard?days=999')).status, 400);
    const d = (await req('/api/metrics/today-views?account=zernio:alpha:alpha')).body;
    assert.equal(d.metrics.views, null); assert.equal(d.availableAccounts, 0); assert.equal(d.partial, true); assert.equal(d.accountViews[0].error, 'intraday_views_unavailable');
    assert.equal(f.calls.length, 2); assert.equal((await req('/api/metrics/media-details')).status, 422);
});
test('scope failure stays partial, healthy scope readable and provider errors redacted', async t => {
    const f = fixture(); const fetch = (url, opts) => opts.headers.Authorization.endsWith('beta') ? response({ error: 'secret-beta arbitrary provider error' }, 401) : f.fetch(url, opts);
    const req = await app(t, ENV, { fetch });
    const config = (await req('/api/metrics/config')).body; assert.equal(config.accountCount, 1); assert.equal(config.partial, true);
    const d = (await req('/api/metrics/dashboard')).body; assert.equal(d.partial, true); assert.equal(d.content.length, 1); assert.equal(d.metrics.views, null);
    assert.doesNotMatch(JSON.stringify(d), /secret-beta|arbitrary provider/);
});
test('same raw account via duplicate tenant keys is counted once', async t => {
    const f = fixture(); const req = await app(t, ENV, { fetch: (url, opts) => new URL(url).pathname === '/api/v1/accounts' ? response({ accounts: [raw('same')], pagination: { pages: 1 } }) : f.fetch(url, opts) });
    const d = (await req('/api/metrics/accounts')).body; assert.equal(d.accounts.length, 1); assert.equal(d.accounts[0].id, 'zernio:alpha:same');
});
test('pagination uses page plus limit, pages metadata, dedup and honest cap', async t => {
    const calls = [];
    const req = await app(t, { ...ENV, ZERNIO_API_KEYS: '', ZERNIO_API_KEY: 'single-secret' }, { fetch: async (input, options) => { const u = new URL(input); calls.push(u); assert.equal(options.headers.Authorization, 'Bearer single-secret'); assert.equal(u.searchParams.get('limit'), '100'); const page = Number(u.searchParams.get('page')); return response({ accounts: [raw('a' + page), raw('duplicate')], pagination: { pages: 11 } }); } });
    const d = (await req('/api/metrics/accounts')).body; assert.equal(calls.length, 10); assert.equal(d.accounts.length, 11); assert.equal(d.truncated, true); assert.equal(d.partial, true);
});
test('fetch timeout is bounded and safe even if transport ignores abort', async t => {
    const req = await app(t, { ...ENV, ZERNIO_API_KEYS: '', ZERNIO_API_KEY: 'single-secret' }, { timeoutMs: 10, fetch: () => new Promise(() => {}) });
    const d = (await req('/api/metrics/config')).body; assert.equal(d.partial, true); assert.equal(d.errors[0].code, 'zernio_timeout');
});
test('Facebook 90 days explicitly partial; Instagram unique reach remains scoped', async t => {
    const f = fixture(), req = await app(t, ENV, f);
    const d = (await req('/api/metrics/dashboard?days=90')).body; assert.equal(d.partial, true); assert.equal(d.accountMetrics[1].metrics.views, null); assert.ok(d.errors.some(e => e.code === 'account_insights_range_unsupported'));
});
test('post normalization refuses cross-account aggregate, unsupported values remain null', () => {
    const a = { id: 'zernio:a:a', providerId: 'a', provider: 'instagram' }, range = { since: 0, until: CLOCK / 1000 };
    const p = { _id: 'p', publishedAt: '2026-09-23', platform: 'instagram', analytics: { views: 999 }, platforms: [{ accountId: 'other', platform: 'instagram', analytics: { views: 888 } }] };
    assert.equal(normalizePost(p, a, range), null);
    p.platforms = [{ accountId: 'a', platform: 'instagram', analytics: { likes: 0 } }];
    const v = normalizePost(p, a, range); assert.equal(v.views, null); assert.equal(v.likes, 0); assert.equal(v.interactions, null);
});

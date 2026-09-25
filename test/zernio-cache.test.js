'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const { createZernioMetrics } = require('../lib/zernio-metrics');
const env = { METRICS_USERNAME: 'u', METRICS_PASSWORD: 'p', ZERNIO_API_KEY: 'test-key', PUBLIC_BASE_URL: 'https://example.com' };
const reply = data => new Response(JSON.stringify(data), { status: 200 });
async function setup(t, transport, clock) {
    const app = express(), adapter = createZernioMetrics(env, { fetch: transport, now: clock }); app.use(adapter);
    const s = app.listen(0, '127.0.0.1'); await new Promise(r => s.once('listening', r));
    t.after(async () => { await adapter.close(); await new Promise(r => s.close(r)); });
    return async path => { const r = await fetch(`http://127.0.0.1:${s.address().port}/api/metrics${path}`, { headers: { authorization: 'Basic ' + Buffer.from('u:p').toString('base64') } }); return { status: r.status, data: await r.json() }; };
}
test('registry refresh invalidates cached membership and changed connection identity', async t => {
    let time = Date.parse('2026-09-24T12:00:00Z'), present = true, posts = 0;
    const req = await setup(t, async input => {
        const u = new URL(input);
        if (u.pathname.endsWith('/accounts')) return reply({ accounts: present ? [{ _id: 'a', platform: 'instagram', isActive: true, followersCount: 8, createdAt: '2026-01-01' }] : [], pagination: { pages: 1 } });
        if (u.pathname.endsWith('/analytics')) { posts++; return reply({ posts: [], pagination: { pages: 1 } }); }
        return reply({ success: true, platform: 'instagram', accountId: 'a', metricType: 'total_value', dateRange: {}, metrics: { reach: { total: 1 }, views: { total: 2 }, total_interactions: { total: 3 } } });
    }, () => time);
    assert.equal((await req('/dashboard?account=zernio:default:a')).status, 200);
    assert.equal((await req('/dashboard?account=zernio:default:a')).status, 200); assert.equal(posts, 1);
    time += 61000; present = false;
    assert.equal((await req('/dashboard?account=zernio:default:a')).status, 404); assert.equal(posts, 1);
});
test('post pagination bounded, deduplicated and marked truncated while period metrics stay distinct', async t => {
    let pages = 0;
    const req = await setup(t, async input => {
        const u = new URL(input);
        if (u.pathname.endsWith('/accounts')) return reply({ accounts: [{ _id: 'a', platform: 'instagram', followersCount: 8 }], pagination: { pages: 1 } });
        if (u.pathname.endsWith('/analytics')) { pages++; return reply({ posts: [{ _id: 'same', platform: 'instagram', publishedAt: '2026-09-23', analytics: { views: 900 } }], pagination: { pages: 6 } }); }
        return reply({ success: true, platform: 'instagram', accountId: 'a', metricType: 'total_value', metrics: { reach: { total: 1 }, views: { total: 2 }, total_interactions: { total: 3 } } });
    }, () => Date.parse('2026-09-24T12:00:00Z'));
    const d = (await req('/dashboard')).data; assert.equal(pages, 5); assert.equal(d.content.length, 1); assert.equal(d.truncated, true); assert.equal(d.partial, true); assert.equal(d.metrics.views, null); assert.equal(d.accountMetrics[0].metrics.views, 2);
});
test('missing follower count invokes scoped follower stats; unsupported selection does not block other ranges', async t => {
    let followers = 0;
    const req = await setup(t, async input => {
        const u = new URL(input);
        if (u.pathname.endsWith('/accounts')) return reply({ accounts: [{ _id: 'a', platform: 'facebook' }], pagination: { pages: 1 } });
        if (u.pathname.endsWith('/analytics')) return reply({ posts: [], pagination: { pages: 1 } });
        if (u.pathname.endsWith('/follower-stats')) { followers++; assert.equal(u.searchParams.get('accountIds'), 'a'); return reply({ accounts: [{ _id: 'other', currentFollowers: 1000 }, { _id: 'a', currentFollowers: 7 }] }); }
        return reply({ success: true, platform: 'facebook', accountId: 'a', metricType: 'total_value', metrics: { page_media_view: { total: 2 }, page_post_engagements: { total: 3 } } });
    }, () => Date.parse('2026-09-24T12:00:00Z'));
    const a = await req('/dashboard?days=90'); assert.equal(a.data.partial, true); assert.equal(a.data.metrics.followers, 7);
    const b = await req('/dashboard?days=7'); assert.equal(b.status, 200); assert.equal(b.data.metrics.views, 2); assert.equal(followers, 2);
});
test('concurrent identical dashboard reads coalesce upstream work', async t => {
    let posts = 0;
    const req = await setup(t, async input => {
        const u = new URL(input);
        if (u.pathname.endsWith('/accounts')) return reply({ accounts: [{ _id: 'a', platform: 'instagram', followersCount: 8 }], pagination: { pages: 1 } });
        if (u.pathname.endsWith('/analytics')) { posts++; await new Promise(r => setTimeout(r, 30)); return reply({ posts: [], pagination: { pages: 1 } }); }
        return reply({ success: true, platform: 'instagram', accountId: 'a', metricType: 'total_value', metrics: { reach: { total: 1 }, views: { total: 2 }, total_interactions: { total: 3 } } });
    }, () => Date.parse('2026-09-24T12:00:00Z'));
    const [a, b] = await Promise.all([req('/dashboard'), req('/dashboard')]); assert.equal(a.status, 200); assert.equal(b.status, 200); assert.equal(posts, 1);
});

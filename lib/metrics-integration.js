'use strict';
const path = require('node:path');
const fs = require('node:fs');
const { timingSafeEqual } = require('node:crypto');

// No network proxy: the vendored engine runs inside this process. Each Tracker
// deployment MUST own a separate private DATA_DIR and encryption key.
function createMetricsIntegration(env = process.env, options = {}) {
    if (env.METRICS_PROVIDER === 'zernio') return require('./zernio-metrics').createZernioMetrics(env, options);
    const invalidProvider = Boolean(env.METRICS_PROVIDER && env.METRICS_PROVIDER !== 'lume') || Boolean((env.ZERNIO_API_KEY || env.ZERNIO_API_KEYS) && env.METRICS_PROVIDER !== 'lume');
    const username = env.METRICS_USERNAME;
    const password = env.METRICS_PASSWORD;
    let enginePromise;
    let closed = false;
    const config = { ...env, PERSONAL_USERNAME: username, PERSONAL_PASSWORD: password };
    const missing = invalidProvider ? ['METRICS_PROVIDER'] : [];
    if (!username) missing.push('METRICS_USERNAME');
    if (!password) missing.push('METRICS_PASSWORD');
    // Reject source-tree storage even if the static allowlist is later relaxed.
    if (env.DATA_DIR) {
        let dir = path.resolve(env.DATA_DIR);
        try { dir = fs.realpathSync(dir); } catch {}
        const root = fs.realpathSync(path.resolve(__dirname, '..'));
        if (!path.isAbsolute(env.DATA_DIR) || dir === root || dir.startsWith(root + path.sep)) missing.push('DATA_DIR_OUTSIDE_REPOSITORY');
    }
    const engine = () => enginePromise ||= import('./lume-server.mjs').then(({ createServer }) => {
        const server = createServer(config, options);
        if (closed) server.emit('close');
        return server;
    });
    const middleware = async (req, res, next) => {
        const pathname = new URL(req.url, 'http://localhost').pathname;
        if (!(pathname === '/api/metrics' || pathname.startsWith('/api/metrics/') || pathname === '/auth' || pathname.startsWith('/auth/'))) return next();
        res.setHeader('Cache-Control', 'no-store');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('Referrer-Policy', 'no-referrer');
        if (missing.length) return res.status(503).json({ error: 'metrics_access_not_configured', missingConfiguration: missing });
        // Browser-native Basic authentication over HTTPS; never JS-managed auth.
        // The engine performs constant-time verification and rate limiting.
        let target = req.url;
        if (pathname === '/auth/start') target = req.url.replace('/auth/start', '/api/auth/start');
        else if (pathname === '/auth/disconnect') target = req.url.replace('/auth/disconnect', '/api/disconnect');
        else if (pathname === '/auth/login') target = '/api/config';
        else if (pathname.startsWith('/api/metrics/')) target = req.url.replace('/api/metrics/', '/api/');
        try {
            const server = await engine();
            if (closed) return res.status(503).json({ error: 'metrics_unavailable' });
            if (pathname === '/auth/login') {
                const supplied = Buffer.from(req.headers.authorization || '');
                const expected = Buffer.from('Basic ' + Buffer.from(username + ':' + password).toString('base64'));
                if (req.method === 'GET' && supplied.length === expected.length && timingSafeEqual(supplied, expected)) return res.redirect(302, '/');
            }
            req.url = target;
            server.emit('request', req, res);
        } catch {
            if (!res.headersSent) res.status(503).json({ error: 'metrics_runtime_unavailable' });
        }
    };
    middleware.close = async () => { closed = true; if (enginePromise) (await enginePromise).emit('close'); };
    return middleware;
}
module.exports = { createMetricsIntegration };

'use strict';
const { createHash, timingSafeEqual } = require('node:crypto');

function metricsEnabled(env) {
    return env.METRICS_ENABLED === 'true' || ['METRICS_SECRETS_FILE', 'METRICS_USERNAME', 'METRICS_PASSWORD', 'META_APP_ID', 'META_APP_SECRET', 'FACEBOOK_APP_ID', 'FACEBOOK_APP_SECRET', 'TOKEN_ENCRYPTION_KEY', 'DATA_DIR'].some(k => Boolean(env[k]));
}
function createPanelSecurity(env = process.env) {
    const enabled = metricsEnabled(env);
    const configured = Boolean(env.METRICS_USERNAME && env.METRICS_PASSWORD);
    const digest = value => createHash('sha256').update(value).digest();
    const expected = digest('Basic ' + Buffer.from(`${env.METRICS_USERNAME}:${env.METRICS_PASSWORD}`).toString('base64'));
    let origin;
    try { const u = new URL(env.PUBLIC_BASE_URL); if (['https:', 'http:'].includes(u.protocol) && u.origin === env.PUBLIC_BASE_URL) origin = u.origin; } catch {}
    return (req, res, next) => {
        if (!enabled) return next(); // Local legacy-only compatibility; never a public deployment mode.
        res.setHeader('Cache-Control', 'no-store');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('Referrer-Policy', 'no-referrer');
        res.setHeader('X-Frame-Options', 'DENY');
        if (!configured) return res.status(503).json({ error: 'panel_access_not_configured' });
        if (!timingSafeEqual(digest(req.headers.authorization || ''), expected)) {
            res.setHeader('WWW-Authenticate', 'Basic realm="Tracker", charset="UTF-8"');
            return res.status(401).json({ error: 'authentication_required' });
        }
        if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method)) {
            if (!origin) return res.status(503).json({ error: 'panel_origin_not_configured' });
            if (req.headers.origin !== origin || req.headers['sec-fetch-site'] === 'cross-site') return res.status(403).json({ error: 'invalid_origin' });
        }
        next();
    };
}
// Return copies: cookies remain write-only and historical disk data stays intact.
function publicAccountData(value) {
    if (Array.isArray(value)) return value.map(publicAccountData);
    if (!value || typeof value !== 'object') return value;
    return Object.fromEntries(Object.entries(value)
        .filter(([key]) => !/(cookie|token|secret|password|authorization|credential)/i.test(key) && key !== '_settings')
        .map(([key, entry]) => [key, publicAccountData(entry)]));
}
module.exports = { createPanelSecurity, metricsEnabled, publicAccountData };

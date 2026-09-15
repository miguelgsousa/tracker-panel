'use strict';
// Server-only bootstrap. Never expose this module or its input via HTTP.
const fs = require('node:fs');
const path = require('node:path');
const KEYS = Object.freeze(['META_APP_ID', 'META_APP_SECRET', 'FACEBOOK_APP_ID', 'FACEBOOK_APP_SECRET']);
const MAX_BYTES = 16 * 1024;
const ERROR = 'Invalid metrics secrets configuration';

function loadMetricsSecrets(env = process.env) {
    if (env.METRICS_SECRETS_FILE === undefined) return;
    let fd;
    try {
        const configured = env.METRICS_SECRETS_FILE;
        if (typeof configured !== 'string' || !path.isAbsolute(configured)) throw new Error();
        const root = fs.realpathSync(path.join(__dirname, '..'));
        const filename = fs.realpathSync(configured);
        const relative = path.relative(root, filename);
        if (relative === '' || (!relative.startsWith('..' + path.sep) && !path.isAbsolute(relative))) throw new Error();
        const parent = fs.statSync(path.dirname(filename));
        // Private directory, owned by the running service. Never change permissions here.
        if (!parent.isDirectory() || (parent.mode & 0o7777) !== 0o700 || parent.uid !== process.geteuid()) throw new Error();
        // NONBLOCK prevents a substituted FIFO from hanging startup; NOFOLLOW rejects final symlinks.
        fd = fs.openSync(filename, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
        const before = fs.fstatSync(fd);
        if (!before.isFile() || before.uid !== process.geteuid() || ![0o400, 0o600].includes(before.mode & 0o7777) || before.nlink !== 1 || before.size < 1 || before.size > MAX_BYTES) throw new Error();
        const buffer = Buffer.alloc(MAX_BYTES + 1);
        let length = 0;
        while (length < buffer.length) {
            const n = fs.readSync(fd, buffer, length, buffer.length - length, null);
            if (!n) break;
            length += n;
        }
        const after = fs.fstatSync(fd);
        if (length > MAX_BYTES || length !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs) throw new Error();
        const data = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(buffer.subarray(0, length)));
        if (!data || Array.isArray(data) || typeof data !== 'object' || Object.keys(data).length === 0) throw new Error();
        for (const [key, value] of Object.entries(data)) {
            if (!KEYS.includes(key) || typeof value !== 'string' || !value.trim() || value.includes('\0')) throw new Error();
        }
        // Validate everything before mutating env. Explicit env (including blank) wins.
        for (const key of KEYS) if (Object.hasOwn(data, key) && env[key] === undefined) env[key] = data[key];
    } catch {
        // No underlying parser/filesystem message, path, contents, or error cause.
        throw new Error(ERROR);
    } finally {
        if (fd !== undefined) fs.closeSync(fd);
    }
}
module.exports = { loadMetricsSecrets, KEYS, MAX_BYTES };

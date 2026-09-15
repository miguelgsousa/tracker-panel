import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { loadMetricsSecrets, KEYS, MAX_BYTES } = require('../lib/metrics-secrets');
const root = path.resolve(import.meta.dirname, '..');
const fake = Object.fromEntries(KEYS.map(k => [k, `fixture-only-${k}`]));
const message = 'Invalid metrics secrets configuration';
function fixture(t, content = JSON.stringify(fake)) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tracker-secrets-'));
    fs.chmodSync(dir, 0o700);
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, content, { mode: 0o600 });
    return { dir, file, env: { METRICS_SECRETS_FILE: file } };
}
function invalid(env) {
    const before = { ...env };
    assert.throws(() => loadMetricsSecrets(env), e => e.message === message && e.cause === undefined);
    assert.deepEqual(env, before, 'failure must not partially populate environment');
}
test('absent reference is a no-op; valid file loads all four app settings only', t => {
    const env = {}; loadMetricsSecrets(env); assert.deepEqual(env, {});
    const f = fixture(t); loadMetricsSecrets(f.env);
    assert.deepEqual(f.env, { METRICS_SECRETS_FILE: f.file, ...fake });
});
test('explicit environment wins, including intentional blank; read-only mode supported', t => {
    const f = fixture(t); fs.chmodSync(f.file, 0o400);
    f.env.META_APP_ID = 'explicit-fixture'; f.env.META_APP_SECRET = '';
    loadMetricsSecrets(f.env);
    assert.equal(f.env.META_APP_ID, 'explicit-fixture'); assert.equal(f.env.META_APP_SECRET, '');
    assert.equal(f.env.FACEBOOK_APP_SECRET, fake.FACEBOOK_APP_SECRET);
});
test('single-provider nonempty subset is allowed', t => {
    const f = fixture(t, JSON.stringify({ META_APP_ID: 'fixture' })); loadMetricsSecrets(f.env);
    assert.equal(f.env.META_APP_ID, 'fixture'); assert.equal(f.env.FACEBOOK_APP_ID, undefined);
});
for (const [name, content] of Object.entries({
    malformed: '{"META_APP_SECRET":"PRIVATE_PARSE_MARKER",', emptyObject: '{}', array: '[]', null: 'null',
    number: '7', unexpected: JSON.stringify({ ...fake, TOKEN_ENCRYPTION_KEY: 'not-allowed' }),
    prototype: '{"__proto__":"no"}', blank: JSON.stringify({ ...fake, META_APP_ID: '  ' }),
    wrongType: JSON.stringify({ ...fake, META_APP_ID: 123 }), nul: JSON.stringify({ META_APP_ID: '\0' }),
    oversized: ' '.repeat(MAX_BYTES + 1), empty: '', invalidUtf8: Buffer.from([0xff])
})) test(`reject ${name} without exposing contents or partially loading`, t => invalid(fixture(t, content).env));
for (const mode of [0o644, 0o640, 0o660, 0o700, 0o4600]) test(`reject unsafe file mode ${mode.toString(8)}`, t => {
    const f = fixture(t); fs.chmodSync(f.file, mode); invalid(f.env);
});
test('reject unsafe parent directory without changing permissions', t => {
    const f = fixture(t); fs.chmodSync(f.dir, 0o755); invalid(f.env);
    assert.equal(fs.statSync(f.dir).mode & 0o777, 0o755);
});
test('reject missing, empty, relative, URL and directory references', t => {
    const f = fixture(t);
    for (const p of ['', 'relative.json', 'https://example.com/secret', f.file + '.missing', f.dir]) invalid({ METRICS_SECRETS_FILE: p });
});
test('reject FIFO without blocking and hardlinked files', t => {
    const f = fixture(t); const link = path.join(f.dir, 'hardlink'); fs.linkSync(f.file, link); invalid(f.env);
    const fifo = path.join(f.dir, 'fifo'); assert.equal(spawnSync('mkfifo', [fifo]).status, 0);
    invalid({ METRICS_SECRETS_FILE: fifo });
});
test('reject repository realpaths, including symlinks from an external private directory', t => {
    const f = fixture(t); const dir = fs.mkdtempSync(path.join(root, '.secrets-test-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'fixture.json'); fs.writeFileSync(file, JSON.stringify(fake), { mode: 0o600 });
    invalid({ METRICS_SECRETS_FILE: file });
    const link = path.join(f.dir, 'symlink'); fs.symlinkSync(file, link); invalid({ METRICS_SECRETS_FILE: link });
});
test('explicit environment cannot hide an invalid configured file', t => {
    invalid({ ...fixture(t, 'PRIVATE_PARSE_MARKER').env, ...fake });
});
function child(file, script) {
    // Never inherit real provider/access/store settings into fixture subprocesses.
    return spawnSync(process.execPath, ['-e', script], {
        cwd: root, env: { PATH: process.env.PATH, METRICS_SECRETS_FILE: file, HOST: '127.0.0.1', PORT: '0' },
        encoding: 'utf8', timeout: 15000
    });
}
test('invalid reference stops actual server bootstrap with generic error, no listener', t => {
    const f = fixture(t, 'PRIVATE_PARSE_MARKER');
    const result = child(f.file, "require('./server.js')");
    assert.equal(result.status, 1); assert.equal(result.stdout, '');
    assert.ok(result.stderr.includes(message)); assert.ok(!result.stderr.includes('PRIVATE_PARSE_MARKER'));
    assert.ok(!result.stderr.includes(f.file));
});
test('actual server loads before security snapshot; file-only app config gates entire panel', t => {
    const f = fixture(t);
    const result = child(f.file, `
      const assert = require('node:assert/strict');
      const { app, metricsIntegration } = require('./server.js');
      const server = app.listen(0, '127.0.0.1', async () => {
        try {
          assert.ok(require('./lib/metrics-secrets').KEYS.every(k => Boolean(process.env[k])));
          for (const p of ['/', '/api/accounts', '/metrics.js', '/api/metrics/config', '/tracker-panel-meta.json', '/lib/metrics-secrets.js']) {
            const response = await fetch('http://127.0.0.1:' + server.address().port + p);
            assert.equal(response.status, 503);
            assert.ok(!(await response.text()).includes('fixture-only'));
          }
          console.log('bootstrap-gate-verified');
        } catch { process.exitCode = 1; }
        finally { await metricsIntegration.close(); server.close(); }
      });
    `);
    assert.equal(result.status, 0, result.stderr); assert.match(result.stdout, /bootstrap-gate-verified/);
});

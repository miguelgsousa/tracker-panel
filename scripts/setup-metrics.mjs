#!/usr/bin/env node
// Offline bootstrap only: no provider requests, secret downloads or service startup.
import fs from 'node:fs';
import path from 'node:path';
import {homedir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
const {loadMetricsSecrets} = require('../lib/metrics-secrets.js');
const root = fs.realpathSync(fileURLToPath(new URL('../', import.meta.url)));
const usage = 'Usage: npm run setup:metrics -- --origin https://YOUR-TRACKER-HOST --secrets-file /private/tracker-meta.json [--output-dir /private/NEW-tracker-runtime]';
const fail = message => { throw new Error(message); };
function safePath(value) {
  // Single-quoted dotenv and shell values must round-trip without interpolation.
  if (!path.isAbsolute(value) || /[\x00-\x1f\x7f'"\\]/.test(value)) fail('Use absolute paths without quotes, backslashes or control characters.');
  return path.resolve(value);
}
function outsideRepository(value) {
  const relative = path.relative(root, value);
  if (relative === '' || (!relative.startsWith('..'+path.sep) && !path.isAbsolute(relative))) fail('Runtime output must be outside the repository.');
}
function main() {
  const [major,minor] = process.versions.node.split('.').map(Number);
  if (major < 22 || (major === 22 && minor < 13)) fail('Node >=22.13 is required.');
  if (typeof process.geteuid !== 'function') fail('Setup requires POSIX file ownership and permissions.');
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === '--help') { console.log(usage); return; }
  const options = {};
  for (let i=0; i<args.length; i+=2) {
    const key=args[i], value=args[i+1];
    if (!['--origin','--secrets-file','--output-dir'].includes(key) || Object.hasOwn(options,key) || !value || value.startsWith('--')) fail(usage);
    options[key]=value;
  }
  if (!options['--origin'] || !options['--secrets-file']) fail(usage);
  let origin;
  try { origin = new URL(options['--origin']); } catch { fail('Supply an exact HTTPS origin, without credentials, path, slash, query or fragment.'); }
  // The embedded engine requires HTTPS, including for localhost OAuth callbacks.
  if (/[\x00-\x20\x7f'"\\]/.test(options['--origin']) || origin.protocol !== 'https:' || origin.origin !== options['--origin'] || origin.username || origin.password) fail('Supply an exact HTTPS origin, without credentials, path, slash, query or fragment.');
  const secrets = safePath(options['--secrets-file']);
  const validated = {METRICS_SECRETS_FILE:secrets};
  loadMetricsSecrets(validated); // Existing allowlist, ownership, mode and bounded-read checks.
  for (const prefix of ['META','FACEBOOK']) {
    if (Boolean(validated[prefix+'_APP_ID']) !== Boolean(validated[prefix+'_APP_SECRET'])) fail('Each configured provider requires both its app ID and app secret.');
  }
  const requested = safePath(options['--output-dir'] || path.join(homedir(),'tracker-metrics-'+randomBytes(8).toString('hex')));
  // Resolve the existing parent BEFORE checking the repository boundary (symlink aliases).
  let parent;
  try {
    parent = fs.realpathSync(path.dirname(requested));
    const info = fs.statSync(parent);
    if (!info.isDirectory() || info.uid !== process.geteuid() || (info.mode & 0o022)) throw new Error();
  } catch { fail('Output parent must already exist, be owned by this service user and not be group/world writable.'); }
  const output = safePath(path.join(parent,path.basename(requested)));
  outsideRepository(output);
  process.umask(0o077);
  const filename = path.join(output,'metrics.env');
  try {
    // Nonrecursive exclusive directory creation rejects even empty existing directories/symlinks.
    fs.mkdirSync(output,{mode:0o700});
    const data = path.join(output,'metrics-data');
    const legacy = path.join(output,'tracker-data');
    fs.mkdirSync(data,{mode:0o700}); fs.mkdirSync(legacy,{mode:0o700});
    const values = {
      HOST:'127.0.0.1', PORT:'3000', PUBLIC_BASE_URL:origin.origin, METRICS_ENABLED:'true',
      METRICS_USERNAME:'tracker-'+randomBytes(12).toString('hex'),
      METRICS_PASSWORD:randomBytes(32).toString('base64url'),
      TOKEN_ENCRYPTION_KEY:randomBytes(32).toString('hex'),
      METRICS_SECRETS_FILE:secrets, DATA_DIR:data, DB_PATH:legacy,
      API_VERSION:'v25.0', FACEBOOK_API_VERSION:'v26.0'
    };
    fs.writeFileSync(filename,'# Private Tracker runtime. Keep this file and both stores outside Git.\n'+Object.entries(values).map(([key,value])=>`${key}='${value}'`).join('\n')+'\n',{flag:'wx',mode:0o600});
  } catch { fail('Could not create a NEW private runtime. Nothing existing was overwritten or removed. Check the output path and permissions; do not rerun over an existing runtime.'); }
  // Only file reference and shell-safe command; never emit generated or loaded values.
  console.log('Private env: '+filename);
  console.log(`Start (from the Tracker checkout): node --env-file='${filename}' server.js`);
}
try { main(); } catch (error) {
  // Only intentionally generic errors. Never include loader/file contents or OS error details.
  const safe = [usage,'Node >=22.13 is required.','Setup requires POSIX file ownership and permissions.','Use absolute paths without quotes, backslashes or control characters.','Runtime output must be outside the repository.','Supply an exact HTTPS origin, without credentials, path, slash, query or fragment.','Invalid metrics secrets configuration','Each configured provider requires both its app ID and app secret.','Output parent must already exist, be owned by this service user and not be group/world writable.','Could not create a NEW private runtime. Nothing existing was overwritten or removed. Check the output path and permissions; do not rerun over an existing runtime.'];
  console.error(safe.includes(error.message) ? error.message : 'Metrics setup failed; no secret values are logged.');
  process.exitCode=1;
}

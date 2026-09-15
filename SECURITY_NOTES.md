# Security and verification notes

## Secrets and private files

Use a private environment file outside the checkout. Do not commit production app secrets, encryption keys, cookies or analytics databases. `.gitignore` and `.dockerignore` exclude these files. The application serves an explicit public-asset allowlist rather than the repository directory.

Metrics require HTTPS and server-side authentication. OAuth access tokens are encrypted in a dedicated Tracker data store; this integration does not copy the running Lume database. Review `METRICS_BACKEND.md` before deployment.

## Dependency audit

The original dependency lock reported nine advisories. Compatible dependency updates reduced that to three high-severity reports in a single transitive chain: `puppeteer-core` → `@puppeteer/browsers` → `extract-zip` (GHSA-jmr9-qjv8-65gv and GHSA-7pqw-9j4j-h8q3). At the time of verification, npm's proposed remaining fix upgrades Puppeteer to major version 25. That breaking upgrade is not included in this metrics integration.

The Tracker uses a separately installed Chromium executable through puppeteer-core. It does not intentionally use the affected browser-archive download/extraction feature. This usage observation is not a claim that the advisories are fixed. Avoid feeding untrusted archives to the browser installer, and validate a Puppeteer major upgrade separately against the existing platform scrapers.

Run `npm audit` again before production deployment; advisory status can change. Metrics tests use explicit provider mocks and isolated temporary stores. Local browser tests do not establish real Meta permissions or consent.

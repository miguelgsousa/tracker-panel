const express = require('express');
const cors = require('cors');
const { execFile } = require('child_process');
const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-core');

// Chrome executable path — auto-detect OS
const CHROME_PATH = process.env.CHROME_PATH
    || (process.platform === 'win32'
        ? 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
        : '/usr/bin/chromium');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_FILE = process.env.DB_PATH
    ? path.join(process.env.DB_PATH, 'accounts.json')
    : path.join(__dirname, 'accounts.json');
const YT_DLP = process.env.YT_DLP_PATH
    || (process.platform === 'win32'
        ? path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python314', 'Scripts', 'yt-dlp.exe')
        : 'yt-dlp');

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// --- Number formatter for logs ---
function fmt(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
    return String(n);
}

// Parse metric strings like "1.2M", "500K", "12,345" to numbers
function parseMetricStr(str) {
    if (!str) return 0;
    str = String(str).replace(/,/g, '').trim();
    const m = str.match(/([\d.]+)\s*([KMBkmb])?/);
    if (!m) return 0;
    let n = parseFloat(m[1]);
    if (m[2]) {
        const suffix = m[2].toUpperCase();
        if (suffix === 'K') n *= 1000;
        else if (suffix === 'M') n *= 1000000;
        else if (suffix === 'B') n *= 1000000000;
    }
    return Math.round(n);
}

// --- Database helpers ---
function loadDB() {
    try {
        if (fs.existsSync(DB_FILE)) return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
    } catch (e) { console.error('DB read error:', e.message); }
    return { youtube: [], tiktok: [], instagram: [], facebook: [], twitter: [], _folders: { youtube: [], tiktok: [], instagram: [], facebook: [], twitter: [] } };
}

function saveDB(db) {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf8');
}

// --- yt-dlp wrapper ---
function runYtDlp(args, timeout = 30000) {
    return new Promise((resolve, reject) => {
        // Try the full path first, fall back to just 'yt-dlp'
        const bin = fs.existsSync(YT_DLP) ? YT_DLP : 'yt-dlp';
        console.log(`[yt-dlp] Running: ${bin} ${args.join(' ')}`);
        execFile(bin, args, { timeout, windowsHide: true }, (err, stdout, stderr) => {
            if (err) {
                console.error(`[yt-dlp] Error:`, err.message);
                return reject(err);
            }
            resolve(stdout);
        });
    });
}

// Extract JSON metadata from a URL
async function extractInfo(url, extraArgs = []) {
    const args = [
        '--dump-json',
        '--no-download',
        '--no-warnings',
        '--no-playlist',
        ...extraArgs,
        url
    ];
    const output = await runYtDlp(args, 45000);
    return JSON.parse(output.trim().split('\n')[0]);
}

// Extract playlist/channel info (multiple entries)
async function extractPlaylist(url, maxItems = 5, extraArgs = []) {
    const args = [
        '--dump-json',
        '--no-download',
        '--no-warnings',
        '--flat-playlist',
        '--playlist-end', String(maxItems),
        ...extraArgs,
        url
    ];
    const output = await runYtDlp(args, 60000);
    return output.trim().split('\n').filter(Boolean).map(line => {
        try { return JSON.parse(line); } catch { return null; }
    }).filter(Boolean);
}

// --- URL builders per platform ---
function buildUrl(platform, handle) {
    handle = handle.replace(/^@/, '');
    switch (platform) {
        case 'youtube':
            if (handle.startsWith('UC') && handle.length > 20) return `https://www.youtube.com/channel/${handle}`;
            return `https://www.youtube.com/@${handle}`;
        case 'tiktok':
            return `https://www.tiktok.com/@${handle}`;
        case 'instagram':
            return `https://www.instagram.com/${handle}/`;
        case 'facebook':
            // If user pasted a full URL, use it directly
            if (handle.startsWith('http://') || handle.startsWith('https://')) return handle;
            if (handle.includes('facebook.com')) return `https://${handle}`;
            return `https://www.facebook.com/${handle}`;
        case 'twitter':
            return `https://twitter.com/${handle}`;
        default:
            return handle;
    }
}

// Utility to process items concurrently in batches
async function processInBatches(items, batchSize, processFn, onBatchComplete) {
    let results = [];
    for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        const batchResults = await Promise.all(batch.map((item, idx) => processFn(item, i + idx)));
        results = results.concat(batchResults);
        if (onBatchComplete) await onBatchComplete(batchResults, i + batchSize, items.length);
    }
    return results;
}

// --- API Routes ---

// --- Settings API (Facebook Token) ---
app.get('/api/settings/facebook-token', (req, res) => {
    const db = loadDB();
    const token = db._settings?.facebookToken || '';
    res.json({ token: token ? '••••' + token.slice(-8) : '', hasToken: !!token });
});

app.post('/api/settings/facebook-token', (req, res) => {
    const { token } = req.body;
    const db = loadDB();
    if (!db._settings) db._settings = {};
    db._settings.facebookToken = token || '';
    saveDB(db);
    console.log(`[Settings] Facebook API token ${token ? 'saved' : 'removed'}`);
    res.json({ success: true, hasToken: !!token });
});

// --- Folders API ---
app.get('/api/folders', (req, res) => {
    const db = loadDB();
    if (!db._folders) db._folders = { youtube: [], tiktok: [], instagram: [], facebook: [], twitter: [] };
    res.json(db._folders);
});

app.post('/api/folders/:platform', (req, res) => {
    const { platform } = req.params;
    const { name } = req.body;
    const db = loadDB();
    if (!db._folders) db._folders = {};
    if (!db._folders[platform]) db._folders[platform] = [];
    const folder = { id: 'f_' + Date.now(), name, platform };
    db._folders[platform].push(folder);
    saveDB(db);
    res.json(folder);
});

app.patch('/api/accounts/:platform/:id/folder', (req, res) => {
    const { platform, id } = req.params;
    const { folderId } = req.body;
    const db = loadDB();
    const account = (db[platform] || []).find(a => a.id === id);
    if (account) {
        account.folderId = folderId;
        saveDB(db);
    }
    res.json({ success: true });
});

app.delete('/api/folders/:platform/:id', (req, res) => {
    const { platform, id } = req.params;
    const db = loadDB();
    if (db._folders && db._folders[platform]) {
        db._folders[platform] = db._folders[platform].filter(f => f.id !== id);
        // Remove accounts from this folder
        (db[platform] || []).forEach(a => { if (a.folderId === id) a.folderId = null; });
        saveDB(db);
    }
    res.json({ success: true });
});


// GET all accounts
app.get('/api/accounts', (req, res) => {
    res.json(loadDB());
});

// GET accounts for a specific platform
app.get('/api/accounts/:platform', (req, res) => {
    const db = loadDB();
    const platform = req.params.platform;
    res.json(db[platform] || []);
});

// POST add account
app.post('/api/accounts/:platform', (req, res) => {
    const { platform } = req.params;
    const { handle, name } = req.body;

    if (!handle) return res.status(400).json({ error: 'Handle is required' });

    const db = loadDB();
    if (!db[platform]) db[platform] = [];

    const exists = db[platform].find(a => a.handle.toLowerCase() === handle.toLowerCase());
    if (exists) return res.status(409).json({ error: 'Account already exists' });

    const account = {
        id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
        handle: handle.replace(/^@/, ''),
        name: name || handle,
        platform,
        url: buildUrl(platform, handle),
        addedAt: new Date().toISOString(),
        lastFetch: null,
        metrics: null,
        recentContent: [],
        cookie: req.body.cookie || null
    };

    db[platform].push(account);
    saveDB(db);
    res.status(201).json(account);
});

// DELETE account
app.delete('/api/accounts/:platform/:id', (req, res) => {
    const { platform, id } = req.params;
    const db = loadDB();
    if (!db[platform]) return res.status(404).json({ error: 'Platform not found' });
    db[platform] = db[platform].filter(a => a.id !== id);
    saveDB(db);
    res.json({ success: true });
});

// PATCH update account cookie
app.patch('/api/accounts/:platform/:id/cookie', (req, res) => {
    const { platform, id } = req.params;
    const { cookie } = req.body;
    const db = loadDB();
    const account = (db[platform] || []).find(a => a.id === id);
    if (!account) return res.status(404).json({ error: 'Account not found' });

    account.cookie = cookie || null;
    saveDB(db);
    res.json({ success: true });
});

// POST fetch metrics for a single account
app.post('/api/fetch/:platform/:id', async (req, res) => {
    const { platform, id } = req.params;
    const db = loadDB();
    const account = (db[platform] || []).find(a => a.id === id);
    if (!account) return res.status(404).json({ error: 'Account not found' });

    try {
        console.log(`\n[Fetch] Fetching metrics for ${platform}/@${account.handle}...`);
        const url = account.url;

        // Start HTTP Streaming for real-time frontend UI updates
        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        res.setHeader('Transfer-Encoding', 'chunked');

        account.recentContent = [];
        let recentContent = []; // local array for IG/FB/Twitter; YouTube/TikTok write to account.recentContent directly
        let metrics = { totalRecentViews: 0, totalRecentLikes: 0, totalRecentComments: 0, videoCount: 0 };
        account.metrics = metrics;

        if (platform === 'youtube') {
            let totalViews = 0;
            let totalLikes = 0;
            let totalComments = 0;
            let subscriberCount = 0;
            let avatarUrl = null;

            // Fetch videos, shorts and avatar ALL concurrently for maximum speed
            const [videoEntries, shortEntries, channelHtml] = await Promise.all([
                extractPlaylist(url + '/videos', 30).catch(() => []),
                extractPlaylist(url + '/shorts', 30).catch(() => []),
                fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36' } })
                    .then(r => r.text()).catch(() => '')
            ]);

            // Parse avatar
            const avatarMatch = channelHtml.match(/<meta property="og:image" content="([^"]+)">/i) || channelHtml.match(/"avatar":\{"thumbnails":\[\{"url":"([^"]+)"/i);
            if (avatarMatch) {
                avatarUrl = avatarMatch[1].replace(/\\u002F/g, '/').replace(/=s\d+-/i, '=s176-');
            }

            // Tag and merge entries, deduplicating by id
            const seen = new Set();
            const allEntries = [];
            for (const e of videoEntries) { if (!seen.has(e.id)) { seen.add(e.id); allEntries.push({ ...e, _type: 'video' }); } }
            for (const e of shortEntries) { if (!seen.has(e.id)) { seen.add(e.id); allEntries.push({ ...e, _type: 'short' }); } }

            console.log(`  [YouTube] Found ${videoEntries.length} videos + ${shortEntries.length} shorts = ${allEntries.length} total, fetching in batches...`);

            // Initialize early avatar/metrics to UI
            account.metrics.avatar = avatarUrl;
            account.metrics.subscribers = subscriberCount;
            saveDB(db);
            res.write(JSON.stringify({ status: 'update', progress: 0 }) + '\n');

            // Get detailed info for ALL content (videos + shorts) in concurrent batches of 10
            await processInBatches(allEntries, 10, async (entry) => {
                try {
                    const contentUrl = entry._type === 'short'
                        ? `https://www.youtube.com/shorts/${entry.id}`
                        : `https://www.youtube.com/watch?v=${entry.id}`;
                    const info = await extractInfo(contentUrl);
                    return { entry, info };
                } catch (e) {
                    console.error(`  [${entry._type === 'short' ? 'Short' : 'Video'} Error] ${entry.id}:`, e.message);
                    return null;
                }
            }, async (batchResults, currentCount, totalCount) => {
                for (const resItem of batchResults) {
                    if (!resItem) continue;
                    const { entry, info } = resItem;
                    const isShort = entry._type === 'short';
                    const contentUrl = isShort
                        ? `https://www.youtube.com/shorts/${entry.id}`
                        : `https://www.youtube.com/watch?v=${entry.id}`;
                    const views = info.view_count || 0;
                    const likes = info.like_count || 0;
                    const comments = info.comment_count || 0;

                    if (info.channel_follower_count) subscriberCount = info.channel_follower_count;

                    totalViews += views;
                    totalLikes += likes;
                    totalComments += comments;

                    account.recentContent.push({
                        id: info.id,
                        title: info.title,
                        url: contentUrl,
                        thumbnail: info.thumbnail,
                        views,
                        likes,
                        comments,
                        duration: info.duration,
                        durationStr: info.duration_string,
                        uploadDate: info.upload_date,
                        description: (info.description || '').slice(0, 200),
                        type: isShort ? 'short' : 'video'
                    });
                }

                const engagementRate = totalViews > 0 ? ((totalLikes + totalComments) / totalViews * 100).toFixed(2) : 0;
                account.metrics = {
                    avatar: avatarUrl,
                    subscribers: subscriberCount,
                    totalRecentViews: totalViews,
                    totalRecentLikes: totalLikes,
                    totalRecentComments: totalComments,
                    engagementRate: parseFloat(engagementRate),
                    videoCount: account.recentContent.filter(c => c.type === 'video').length,
                    shortCount: account.recentContent.filter(c => c.type === 'short').length,
                    totalCount: account.recentContent.length
                };
                saveDB(db);
                res.write(JSON.stringify({ status: 'update', progress: Math.round((currentCount / totalCount) * 100) }) + '\n');
            });

        } else if (platform === 'tiktok') {
            // Fetch up to 40 videos from TikTok profile to get comprehensive metrics
            const maxVideos = parseInt(req.query.max) || 40;

            let totalViews = 0, totalLikes = 0, totalComments = 0, totalShares = 0;
            let followerCount = 0;
            let avatarUrl = null;

            // Prepare cookie args for yt-dlp if account has cookies
            let ytdlpCookieArgs = [];
            let cookieTmpFile = null;
            if (account.cookie) {
                try {
                    const os = require('os');
                    cookieTmpFile = path.join(os.tmpdir(), `tk_cookies_${account.id}.txt`);
                    const cookiePairs = account.cookie.replace(/[\r\n]+/g, ' ').trim().split(/;\s*/);
                    let cookieContent = '# Netscape HTTP Cookie File\n';
                    for (const pair of cookiePairs) {
                        const [name, ...valParts] = pair.split('=');
                        if (name && valParts.length > 0) {
                            cookieContent += `.tiktok.com\tTRUE\t/\tTRUE\t0\t${name.trim()}\t${valParts.join('=').trim()}\n`;
                        }
                    }
                    fs.writeFileSync(cookieTmpFile, cookieContent);
                    ytdlpCookieArgs = ['--cookies', cookieTmpFile];
                    console.log(`  [TikTok] Cookie file created for yt-dlp`);
                } catch (e) {
                    console.log(`  [TikTok] Failed to create cookie file: ${e.message}`);
                }
            }

            // Strategy 1: Try getting profile data from various sources
            // 1a: TikTok API endpoint (works without login)
            const handle = account.handle.replace(/^@/, '');
            try {
                const apiUrl = `https://www.tiktok.com/@${handle}?isUniqueId=true&isSecUid=false`;
                const apiRes = await fetch(apiUrl, {
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                        ...(account.cookie ? { 'Cookie': account.cookie.replace(/[\r\n]+/g, ' ').trim() } : {})
                    },
                    signal: AbortSignal.timeout(15000)
                });
                const profilePage = await apiRes.text();

                // Extract from SIGI_STATE or __UNIVERSAL_DATA_FOR_REHYDRATION__
                const sigiMatch = profilePage.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/i) ||
                    profilePage.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/i);
                if (sigiMatch) {
                    try {
                        const sigiData = JSON.parse(sigiMatch[1]);
                        // SIGI_STATE format
                        const userModule = sigiData.UserModule || sigiData['__DEFAULT_SCOPE__']?.['webapp.user-detail'];
                        if (userModule) {
                            const users = userModule.users || {};
                            const stats = userModule.stats || {};
                            const userKey = Object.keys(users)[0] || handle;
                            if (stats[userKey]) {
                                followerCount = stats[userKey].followerCount || 0;
                                console.log(`  [TikTok] API: ${followerCount} followers`);
                            }
                            if (users[userKey]) {
                                avatarUrl = users[userKey].avatarLarger || users[userKey].avatarMedium || users[userKey].avatarThumb || null;
                                if (avatarUrl) console.log(`  [TikTok] API: avatar found`);
                            }
                        }
                        // __UNIVERSAL_DATA_FOR_REHYDRATION__ format
                        const userDetail = sigiData['__DEFAULT_SCOPE__']?.['webapp.user-detail']?.userInfo;
                        if (userDetail) {
                            followerCount = userDetail.stats?.followerCount || followerCount;
                            avatarUrl = userDetail.user?.avatarLarger || userDetail.user?.avatarMedium || avatarUrl;
                            console.log(`  [TikTok] Universal data: ${followerCount} followers`);
                        }
                    } catch (e) {
                        console.log(`  [TikTok] Failed to parse SIGI/Universal data: ${e.message}`);
                    }
                }

                // Fallback: try meta tags
                if (!followerCount) {
                    const fmatch = profilePage.match(/followerCount["\s:]+(\d+)/i);
                    if (fmatch) followerCount = parseInt(fmatch[1]);
                }
                if (!avatarUrl) {
                    const amatch = profilePage.match(/"avatarThumb":"([^"]+)"/i) ||
                        profilePage.match(/"avatarLarger":"([^"]+)"/i) ||
                        profilePage.match(/property="og:image" content="([^"]+)"/i);
                    if (amatch) avatarUrl = amatch[1].replace(/\\u002F/g, '/');
                }

                console.log(`  [TikTok] Profile scrape: followers=${followerCount}, avatar=${avatarUrl ? 'YES' : 'NO'}`);
            } catch (e) {
                console.log(`  [TikTok] Profile fetch failed (timeout/blocked): ${e.message}`);
            }

            // Fetch playlist entries
            const entries = await extractPlaylist(url, maxVideos, ytdlpCookieArgs).catch(e => {
                console.error(`  [TikTok] Playlist extraction failed: ${e.message}`);
                return [];
            });

            console.log(`  [TikTok] Found ${entries.length} entries, fetching all details in batches...`);

            // Check if entries contain channel_follower_count (sometimes available in flat-playlist)
            for (const entry of entries) {
                if (entry.channel_follower_count && entry.channel_follower_count > followerCount) {
                    followerCount = entry.channel_follower_count;
                }
                if (entry.uploader_follower_count && entry.uploader_follower_count > followerCount) {
                    followerCount = entry.uploader_follower_count;
                }
            }

            // Initialize early avatar/metrics to UI
            account.metrics = account.metrics || {};
            account.metrics.avatar = avatarUrl;
            account.metrics.followers = followerCount;
            saveDB(db);
            res.write(JSON.stringify({ status: 'update', progress: 0 }) + '\n');

            // Fetch video details in concurrent batches
            let blockedCount = 0;
            await processInBatches(entries, 10, async (entry, i) => {
                try {
                    const videoUrl = entry.url || `https://www.tiktok.com/@${account.handle}/video/${entry.id}`;
                    const info = await extractInfo(videoUrl, ytdlpCookieArgs);
                    return { entry, info, i, videoUrl };
                } catch (e) {
                    if (e.message.includes('blocked')) blockedCount++;
                    console.error(`  [TikTok Error] ${entry.id}:`, e.message.substring(0, 100));
                    // Even if blocked, try to use flat-playlist data
                    if (entry.view_count !== undefined) {
                        return { entry, info: entry, i, videoUrl: entry.url, fromFlat: true };
                    }
                    return null;
                }
            }, async (batchResults, currentCount, totalCount) => {
                for (const resItem of batchResults) {
                    if (!resItem) continue;
                    const { entry, info, i, videoUrl } = resItem;

                    if (info.uploader_follower_count) followerCount = Math.max(followerCount, info.uploader_follower_count);
                    if (info.channel_follower_count) followerCount = Math.max(followerCount, info.channel_follower_count);

                    // Try to get avatar from video metadata if not already found
                    if (!avatarUrl && info.thumbnails) {
                        // Look for avatar-like thumbnails (TikTok includes uploader avatar)
                        const avatarThumb = info.thumbnails.find(t => t.url && (t.url.includes('avatar') || t.url.includes('musically')));
                        if (avatarThumb) avatarUrl = avatarThumb.url;
                    }
                    if (!avatarUrl && info.uploader_url) {
                        // Try constructing from the profile
                        const profileId = info.uploader_id || info.channel_id;
                        if (profileId && info.thumbnail) {
                            // Use the first frame thumbnail as a proxy until we get a real avatar
                            // At least yt-dlp sometimes provides channel thumbnails
                        }
                    }

                    const views = info.view_count || 0;
                    const likes = info.like_count || 0;
                    const comments = info.comment_count || 0;
                    const shares = info.repost_count || 0;

                    totalViews += views;
                    totalLikes += likes;
                    totalComments += comments;
                    totalShares += shares;

                    account.recentContent.push({
                        id: info.id || entry.id,
                        title: info.title || info.description || `TikTok #${i + 1}`,
                        description: (info.description || '').slice(0, 300),
                        url: info.webpage_url || videoUrl,
                        thumbnail: info.thumbnail,
                        views, likes, comments, shares,
                        duration: info.duration,
                        durationStr: info.duration_string,
                        uploadDate: info.upload_date,
                        timestamp: info.timestamp
                    });
                }
                account.metrics = {
                    avatar: avatarUrl,
                    followers: followerCount,
                    totalRecentViews: totalViews,
                    totalRecentLikes: totalLikes,
                    totalRecentComments: totalComments,
                    totalShares: totalShares,
                    engagementRate: totalViews > 0 ? parseFloat(((totalLikes + totalComments) / totalViews * 100).toFixed(2)) : 0,
                    videoCount: account.recentContent.length
                };
                saveDB(db);
                res.write(JSON.stringify({ status: 'update', progress: Math.round((currentCount / totalCount) * 100) }) + '\n');
            });

            // Log if IP was blocked
            if (blockedCount > 0) {
                console.log(`  [TikTok] WARNING: ${blockedCount}/${entries.length} videos blocked by IP. Consider adding cookies.`);
            }

            // Cleanup temp cookie file
            if (cookieTmpFile) try { fs.unlinkSync(cookieTmpFile); } catch (e) { }

        } else if (platform === 'instagram') {
            try {
                console.log(`  [Instagram] Using Instaloader...`);

                // Strategy 1: Instaloader Python script (most reliable)
                try {
                    const igResult = await new Promise((resolve, reject) => {
                        const pyBin = 'python';
                        const scriptPath = path.join(__dirname, 'ig_scraper.py');
                        const args = [scriptPath, account.handle];
                        console.log(`  [Instagram] Running: ${pyBin} ${args.join(' ')}`);
                        execFile(pyBin, args, { timeout: 120000, windowsHide: true }, (err, stdout, stderr) => {
                            if (err) {
                                // Script may have printed JSON to stdout even on error exit
                                if (stdout && stdout.trim()) {
                                    try { return resolve(JSON.parse(stdout.trim())); } catch (e2) { }
                                }
                                return reject(new Error(stderr || err.message));
                            }
                            try {
                                resolve(JSON.parse(stdout.trim()));
                            } catch (e) {
                                reject(new Error('Failed to parse instaloader output'));
                            }
                        });
                    });

                    if (igResult.error) {
                        if (igResult.error === 'rate_limited') {
                            throw new Error('Instagram rate limit (429). Aguarde alguns minutos e tente novamente.');
                        }
                        throw new Error(igResult.error);
                    }

                    const followers = igResult.followers || 0;
                    const postCount = igResult.posts_count || 0;
                    const avatarUrl = igResult.profile_pic_url || null;
                    let totalLikes = 0, totalComments = 0, totalViews = 0;

                    for (const post of (igResult.posts || [])) {
                        const views = post.views || 0;
                        const likes = post.likes || 0;
                        const comments = post.comments || 0;
                        totalViews += views;
                        totalLikes += likes;
                        totalComments += comments;

                        recentContent.push({
                            id: post.id,
                            title: post.title || 'Post',
                            url: post.url,
                            thumbnail: post.thumbnail,
                            views, likes, comments,
                            uploadDate: post.upload_date
                        });
                    }

                    metrics = {
                        avatar: avatarUrl,
                        followers,
                        postCount,
                        fullName: igResult.full_name,
                        isPrivate: igResult.is_private,
                        isVerified: igResult.is_verified,
                        totalRecentViews: totalViews,
                        totalRecentLikes: totalLikes,
                        totalRecentComments: totalComments,
                        engagementRate: followers > 0 ? parseFloat(((totalLikes + totalComments) / followers * 100).toFixed(2)) : 0
                    };

                    console.log(`  [Instagram] Instaloader: ${followers} followers, ${recentContent.length} posts`);

                } catch (e) {
                    console.log(`  [Instagram] Instaloader failed: ${e.message}`);

                    // Strategy 2: Scrape HTML meta tags as fallback  
                    const cleanCookie = account.cookie ? account.cookie.replace(/[\r\n]+/g, ' ').trim() : '';
                    try {
                        const profileHtml = await fetch(url, {
                            headers: {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
                                'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9',
                                ...(cleanCookie ? { 'Cookie': cleanCookie } : {})
                            }
                        }).then(r => r.text());

                        const descMatch = profileHtml.match(/content="([\d,.KMBkmb]+)\s*Followers?/i);
                        let followers = descMatch ? parseMetricStr(descMatch[1]) : 0;
                        const postMatch = profileHtml.match(/([\d,.KMBkmb]+)\s*Posts?/i);
                        let postCount = postMatch ? parseMetricStr(postMatch[1]) : 0;
                        const ogImg = profileHtml.match(/<meta property="og:image"\s+content="([^"]+)"/i);

                        if (followers > 0) {
                            metrics = {
                                avatar: ogImg ? ogImg[1] : null,
                                followers, postCount,
                                totalRecentViews: 0, totalRecentLikes: 0, totalRecentComments: 0
                            };
                            console.log(`  [Instagram] HTML fallback: ${followers} followers`);
                        } else {
                            throw new Error('Sem dados nas meta tags');
                        }
                    } catch (e2) {
                        throw new Error('Instaloader e HTML falharam. O Instagram pode estar bloqueando requisições. Tente novamente mais tarde.');
                    }
                }

            } catch (e) {
                metrics = { error: 'Não foi possível acessar o perfil.', message: e.message };
            }

        } else if (platform === 'facebook') {
            try {
                console.log(`  [Facebook] Starting extraction...`);
                const db2 = loadDB();
                const fbToken = db2._settings?.facebookToken || '';
                const handle = account.handle;

                let pageFollowers = 0, pageLikes = 0, avatarUrl = null, pageName = null, pageCategory = null;
                let totalViews = 0, totalLikes = 0, totalComments = 0, totalShares = 0;
                let graphApiSuccess = false;

                // Resolve page identifier
                let pageId = handle;
                if (handle.includes('facebook.com')) {
                    const profileMatch = handle.match(/profile\.php\?id=(\d+)/);
                    if (profileMatch) pageId = profileMatch[1];
                    else {
                        const pathMatch = handle.match(/facebook\.com\/([^/?&#]+)/);
                        if (pathMatch) pageId = pathMatch[1];
                    }
                }

                // ============================================================
                // STRATEGY 1: Graph API (when token is available)
                // ============================================================
                if (fbToken) {
                    try {
                        console.log(`  [Facebook] Strategy 1: Graph API with token...`);
                        const FB_API = 'https://graph.facebook.com/v21.0';

                        // Step 1: Get all pages this user manages to find matching page + page token
                        let pageAccessToken = fbToken; // fallback to user token
                        try {
                            const accountsRes = await fetch(`${FB_API}/me/accounts?fields=id,name,access_token,category,fan_count,followers_count&access_token=${fbToken}`);
                            const accountsData = await accountsRes.json();
                            const managedPages = accountsData.data || [];
                            console.log(`  [Facebook] User manages ${managedPages.length} pages`);

                            if (managedPages.length > 0) {
                                // Try to match by ID or name
                                const matched = managedPages.find(p =>
                                    p.id === pageId ||
                                    p.name?.toLowerCase() === pageId.toLowerCase() ||
                                    p.name?.toLowerCase().includes(pageId.toLowerCase())
                                );

                                if (matched) {
                                    console.log(`  [Facebook] Matched managed page: "${matched.name}" (ID: ${matched.id})`);
                                    pageId = matched.id;
                                    pageAccessToken = matched.access_token || fbToken;
                                } else {
                                    // If pageId is numeric and not matched, try the first page
                                    console.log(`  [Facebook] Page "${pageId}" not found in managed pages. Trying direct access...`);
                                }
                            }
                        } catch (e) {
                            console.log(`  [Facebook] Failed to fetch managed pages: ${e.message}`);
                        }

                        // Step 2: Fetch page info using the best token available
                        const pageFields = 'id,name,about,category,fan_count,followers_count,picture.type(large),cover,link,website,description';
                        const pageRes = await fetch(`${FB_API}/${pageId}?fields=${pageFields}&access_token=${pageAccessToken}`);
                        const pageData = await pageRes.json();

                        if (pageData.error) throw new Error(pageData.error.message || 'Erro Graph API');

                        pageId = pageData.id;
                        pageName = pageData.name || handle;
                        pageFollowers = pageData.followers_count || 0;
                        pageLikes = pageData.fan_count || 0;
                        pageCategory = pageData.category || null;
                        avatarUrl = pageData.picture?.data?.url || null;

                        console.log(`  [Facebook] Page: ${pageName} (ID: ${pageId}) — ${fmt(pageFollowers)} followers, ${fmt(pageLikes)} likes`);

                        account.metrics = { avatar: avatarUrl, followers: pageFollowers, pageLikes, pageName, pageCategory, totalRecentViews: 0, totalRecentLikes: 0, totalRecentComments: 0, videoCount: 0 };
                        saveDB(db);
                        res.write(JSON.stringify({ status: 'update', progress: 10 }) + '\n');

                        // Fetch posts and videos
                        const postsFields = 'id,message,created_time,full_picture,permalink_url,shares,type,likes.summary(true).limit(0),comments.summary(true).limit(0)';
                        const videoFields = 'id,title,description,length,created_time,permalink_url,thumbnails,views,likes.summary(true).limit(0),comments.summary(true).limit(0)';

                        const [postsData, videosData] = await Promise.all([
                            fetch(`${FB_API}/${pageId}/posts?fields=${postsFields}&limit=25&access_token=${pageAccessToken}`).then(r => r.json()).catch(() => ({ data: [] })),
                            fetch(`${FB_API}/${pageId}/videos?fields=${videoFields}&limit=30&access_token=${pageAccessToken}`).then(r => r.json()).catch(() => ({ data: [] }))
                        ]);

                        const posts = (postsData.error ? [] : postsData.data) || [];
                        const videos = (videosData.error ? [] : videosData.data) || [];

                        for (const v of videos) {
                            const views = v.views || 0;
                            const likes = v.likes?.summary?.total_count || 0;
                            const comments = v.comments?.summary?.total_count || 0;
                            totalViews += views; totalLikes += likes; totalComments += comments;
                            let thumbnail = v.thumbnails?.data?.length > 0 ? (v.thumbnails.data.reduce((b, t) => (!b || (t.height || 0) > (b.height || 0)) ? t : b, null)?.uri || v.thumbnails.data[0].uri) : null;
                            recentContent.push({ id: v.id, title: v.title || (v.description || 'Facebook Video').slice(0, 100), description: (v.description || '').slice(0, 300), url: v.permalink_url || `https://www.facebook.com/${v.id}`, thumbnail, views, likes, comments, duration: v.length ? Math.round(v.length) : null, durationStr: v.length ? `${Math.floor(v.length / 60)}:${String(Math.round(v.length % 60)).padStart(2, '0')}` : null, uploadDate: v.created_time ? v.created_time.replace(/-/g, '').slice(0, 8) : null, timestamp: v.created_time ? Math.floor(new Date(v.created_time).getTime() / 1000) : null, type: 'video' });
                        }

                        let postLikes = 0, postComments = 0, postShares = 0;
                        for (const p of posts) { postLikes += p.likes?.summary?.total_count || 0; postComments += p.comments?.summary?.total_count || 0; postShares += p.shares?.count || 0; }

                        metrics = { avatar: avatarUrl, followers: pageFollowers, pageLikes, pageName, pageCategory, totalRecentViews: totalViews, totalRecentLikes: totalLikes, totalRecentComments: totalComments, totalPostLikes: postLikes, totalPostComments: postComments, totalPostShares: postShares, postCount: posts.length, engagementRate: pageFollowers > 0 ? parseFloat(((totalLikes + totalComments + postLikes + postComments) / pageFollowers * 100).toFixed(2)) : 0, videoCount: recentContent.length };
                        account.metrics = metrics;
                        account.recentContent = recentContent;
                        saveDB(db);
                        graphApiSuccess = true;
                        console.log(`  [Facebook] Graph API OK: ${fmt(pageFollowers)} followers, ${recentContent.length} videos, ${posts.length} posts`);
                    } catch (graphErr) {
                        console.log(`  [Facebook] Graph API failed: ${graphErr.message}`);
                    }
                }

                // ============================================================
                // STRATEGY 2: Puppeteer (real browser scraping)
                // ============================================================
                if (!graphApiSuccess) {
                    console.log(`  [Facebook] Strategy 2: Puppeteer real browser scraping...`);
                    res.write(JSON.stringify({ status: 'update', progress: 5 }) + '\n');

                    let browser = null;
                    try {
                        browser = await puppeteer.launch({
                            executablePath: CHROME_PATH,
                            headless: 'new',
                            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--window-size=1920,1080', '--disable-blink-features=AutomationControlled', '--lang=pt-BR,pt']
                        });

                        const page = await browser.newPage();
                        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                        await page.setViewport({ width: 1920, height: 1080 });

                        // Set cookies if available
                        if (account.cookie) {
                            try {
                                const cookiePairs = account.cookie.replace(/[\r\n]+/g, ' ').trim().split(/;\s*/);
                                const cookies = [];
                                for (const pair of cookiePairs) {
                                    const [name, ...valParts] = pair.split('=');
                                    if (name && valParts.length > 0) {
                                        cookies.push({ name: name.trim(), value: valParts.join('=').trim(), domain: '.facebook.com', path: '/' });
                                    }
                                }
                                if (cookies.length > 0) await page.setCookie(...cookies);
                                console.log(`  [Facebook] Cookies set: ${cookies.length} cookies`);
                            } catch (e) { console.log(`  [Facebook] Cookie set failed: ${e.message}`); }
                        }

                        // Navigate to the Facebook page
                        const fbUrl = url.replace(/\/videos\/?$/, '').replace(/\/$/, '');
                        console.log(`  [Facebook] Navigating to: ${fbUrl}`);
                        await page.goto(fbUrl, { waitUntil: 'networkidle2', timeout: 30000 });

                        // Close cookie consent / login popups aggressively
                        try {
                            await page.evaluate(() => {
                                // Remove ALL dialog overlays
                                document.querySelectorAll('[role="dialog"]').forEach(d => d.remove());
                                // Remove login barriers and fixed overlays
                                document.querySelectorAll('[data-testid="royal_login_form"]').forEach(d => {
                                    let parent = d;
                                    for (let i = 0; i < 10 && parent; i++) { parent = parent.parentElement; }
                                    if (parent) parent.remove();
                                });
                                // Remove fixed position overlays blocking the page
                                document.querySelectorAll('div').forEach(d => {
                                    const style = window.getComputedStyle(d);
                                    if (style.position === 'fixed' && style.zIndex > 100 && d.offsetHeight > 300) d.remove();
                                });
                                // Re-enable scrolling
                                document.body.style.overflow = 'auto';
                                document.documentElement.style.overflow = 'auto';
                            });
                            await new Promise(r => setTimeout(r, 1000));
                        } catch (e) { }

                        // Wait a bit for content to settle
                        await new Promise(r => setTimeout(r, 2000));

                        // Also get the raw page HTML source for regex-based extraction
                        const pageHtml = await page.content();

                        // Extract page data from the rendered DOM
                        const pageData = await page.evaluate(() => {
                            const result = { name: null, avatar: null, followers: 0, likes: 0, category: null, videoIds: [] };

                            // Page name from h1 or og:title
                            const h1 = document.querySelector('h1');
                            if (h1) result.name = h1.textContent.trim();
                            if (!result.name) {
                                const ogTitle = document.querySelector('meta[property="og:title"]');
                                if (ogTitle) result.name = ogTitle.content;
                            }
                            // Clean up name (remove "Conta verificada", "Verified" badges text)
                            if (result.name) {
                                result.name = result.name.replace(/\s*(Conta verificada|Verified|Verificada?)\s*/gi, '').trim();
                            }

                            // Avatar from profile picture
                            const ogImg = document.querySelector('meta[property="og:image"]');
                            if (ogImg) result.avatar = ogImg.content;
                            // Also try profile photo SVG/img
                            const profileImgs = document.querySelectorAll('image[xlink\\:href], svg image');
                            profileImgs.forEach(img => {
                                const href = img.getAttribute('xlink:href') || img.getAttribute('href');
                                if (href && href.includes('scontent') && !result.avatar) result.avatar = href;
                            });
                            // Also try img tags with profile pic
                            const imgs = document.querySelectorAll('img[alt]');
                            imgs.forEach(img => {
                                if (img.alt && result.name && img.alt.includes(result.name) && img.src.includes('scontent')) {
                                    result.avatar = img.src;
                                }
                            });

                            // Get ALL text content, look for followers/likes patterns
                            const allText = document.body.innerText;

                            // Helper to parse FB number format: "17 mi" = 17M, "3,5 mil" = 3500, "1.234" = 1234
                            function parseFbNum(numStr, context) {
                                let n = parseFloat(numStr.replace(/\./g, '').replace(',', '.'));
                                if (isNaN(n)) return 0;
                                const ctx = (context || '').toLowerCase();
                                if (ctx.includes(' mi ') || ctx.includes(' mi\n') || ctx.match(/\d\s*mi\b/)) n *= 1000000;
                                else if (ctx.includes('mil') || ctx.includes('k')) n *= 1000;
                                else if (ctx.match(/\dM\b/i)) n *= 1000000;
                                else if (ctx.includes('b') || ctx.includes('bi')) n *= 1000000000;
                                return Math.round(n);
                            }

                            // Followers patterns (PT-BR and EN) - "17 mi seguidores", "3,5 mil followers", etc.
                            const followerPatterns = [
                                /([\d.,]+)\s*(?:mi|mil|K|M|B|bi)?\s*(?:seguidores?|followers?|pessoas?\s*seguem|people\s*follow)/gi,
                                /(?:seguidores?|followers?)\s*[:\s]*([\d.,]+)\s*(?:mi|mil|K|M|B|bi)?/gi
                            ];

                            for (const pat of followerPatterns) {
                                let m;
                                while ((m = pat.exec(allText)) !== null) {
                                    const numStr = m[1] || m[2];
                                    if (numStr) {
                                        const n = parseFbNum(numStr, m[0]);
                                        if (n > result.followers) result.followers = n;
                                    }
                                }
                            }

                            // Likes patterns
                            const likePatterns = [
                                /([\d.,]+)\s*(?:mi|mil|K|M|B|bi)?\s*(?:curtidas?|likes?|pessoas?\s*curtiram|people\s*like)/gi,
                                /(?:curtidas?|likes?)\s*[:\s]*([\d.,]+)\s*(?:mi|mil|K|M|B|bi)?/gi
                            ];
                            for (const pat of likePatterns) {
                                let m;
                                while ((m = pat.exec(allText)) !== null) {
                                    const numStr = m[1] || m[2];
                                    if (numStr) {
                                        const n = parseFbNum(numStr, m[0]);
                                        if (n > result.likes) result.likes = n;
                                    }
                                }
                            }

                            // Category
                            const spans = document.querySelectorAll('span');
                            const categories = ['Restaurante', 'Loja', 'Empresa', 'Serviço', 'Organização', 'Marca', 'Mídia', 'Entretenimento', 'Esporte', 'Restaurant', 'Store', 'Company', 'Media', 'Entertainment', 'Criador', 'Creator'];
                            for (const span of spans) {
                                const txt = span.textContent.trim();
                                if (categories.some(c => txt.toLowerCase().includes(c.toLowerCase())) && txt.length < 60) {
                                    result.category = txt;
                                    break;
                                }
                            }

                            // Collect video IDs from links on the page
                            const links = document.querySelectorAll('a[href]');
                            const vidSet = new Set();
                            for (const link of links) {
                                const href = link.href;
                                let m2;
                                if ((m2 = href.match(/\/videos\/(\d{10,})/))) vidSet.add(m2[1]);
                                if ((m2 = href.match(/watch\/?\?v=(\d{10,})/))) vidSet.add(m2[1]);
                                if ((m2 = href.match(/\/reel\/(\d{10,})/))) vidSet.add(m2[1]);
                            }
                            result.videoIds = [...vidSet].slice(0, 30);

                            return result;
                        });

                        // Also extract followers from raw HTML source (works even when login overlay hides DOM text)
                        if (!pageData.followers) {
                            const htmlFollowerPatterns = [
                                /([\d.,]+)\s*(?:mi|mil|K|M|B)?\s*(?:seguidores|followers|people follow)/gi,
                                /follower[s_]*count["\s:]+(\d+)/gi,
                                /\"followerCount\":(\d+)/gi
                            ];
                            for (const pat of htmlFollowerPatterns) {
                                let m;
                                while ((m = pat.exec(pageHtml)) !== null) {
                                    const numStr = m[1];
                                    let n = parseFloat(numStr.replace(/\./g, '').replace(',', '.'));
                                    if (isNaN(n)) continue;
                                    const ctx = m[0].toLowerCase();
                                    if (ctx.includes(' mi ') || ctx.match(/\d\s*mi\b/)) n *= 1000000;
                                    else if (ctx.includes('mil') || ctx.includes('k')) n *= 1000;
                                    n = Math.round(n);
                                    if (n > pageData.followers) pageData.followers = n;
                                }
                            }
                            if (pageData.followers > 0) console.log(`  [Facebook] HTML source fallback: ${pageData.followers} followers`);
                        }

                        // If we couldn't find much, try the /videos page too
                        let videoPageIds = [];
                        if (pageData.videoIds.length < 5) {
                            try {
                                console.log(`  [Facebook] Navigating to videos page...`);
                                await page.goto(fbUrl + '/videos', { waitUntil: 'networkidle2', timeout: 25000 });
                                await new Promise(r => setTimeout(r, 3000));
                                // Scroll down to load more videos
                                for (let i = 0; i < 3; i++) {
                                    await page.evaluate(() => window.scrollBy(0, 1500));
                                    await new Promise(r => setTimeout(r, 1500));
                                }
                                videoPageIds = await page.evaluate(() => {
                                    const links = document.querySelectorAll('a[href]');
                                    const vidSet = new Set();
                                    for (const link of links) {
                                        const href = link.href;
                                        let m;
                                        if ((m = href.match(/\/videos\/(\d{10,})/))) vidSet.add(m[1]);
                                        if ((m = href.match(/watch\/?\?v=(\d{10,})/))) vidSet.add(m[1]);
                                        if ((m = href.match(/\/reel\/(\d{10,})/))) vidSet.add(m[1]);
                                    }
                                    return [...vidSet];
                                });

                                // Also try to get followers from videos page if main page failed
                                if (!pageData.followers) {
                                    const extraData = await page.evaluate(() => {
                                        const allText = document.body.innerText;
                                        let followers = 0;
                                        const m = allText.match(/([\d.,]+)\s*(?:mil|K|M|B)?\s*(?:seguidores?|followers?|pessoas? seguem|people follow)/i);
                                        if (m) {
                                            let n = parseFloat(m[1].replace(/\./g, '').replace(',', '.'));
                                            if (m[0].toLowerCase().includes('mil') || m[0].includes('K')) n *= 1000;
                                            if (m[0].includes('M')) n *= 1000000;
                                            followers = Math.round(n);
                                        }
                                        return { followers };
                                    });
                                    if (extraData.followers > pageData.followers) pageData.followers = extraData.followers;
                                }
                            } catch (e) {
                                console.log(`  [Facebook] Videos page error: ${e.message}`);
                            }
                        }

                        await browser.close();
                        browser = null;

                        // Merge video IDs
                        const allVideoIds = [...new Set([...pageData.videoIds, ...videoPageIds])].slice(0, 30);

                        pageName = pageData.name || pageId;
                        pageFollowers = pageData.followers || 0;
                        pageLikes = pageData.likes || 0;
                        avatarUrl = pageData.avatar || null;
                        pageCategory = pageData.category || null;

                        console.log(`  [Facebook] Puppeteer: name="${pageName}", ${fmt(pageFollowers)} followers, ${fmt(pageLikes)} likes, ${allVideoIds.length} video IDs`);

                        // Send early update
                        account.metrics = { avatar: avatarUrl, followers: pageFollowers, pageLikes, pageName, pageCategory, totalRecentViews: 0, totalRecentLikes: 0, totalRecentComments: 0, videoCount: 0 };
                        saveDB(db);
                        res.write(JSON.stringify({ status: 'update', progress: 30 }) + '\n');

                        // Fetch video details with yt-dlp
                        if (allVideoIds.length > 0) {
                            let ytdlpCookieArgs = [];
                            let cookieTmpFile = null;
                            if (account.cookie) {
                                try {
                                    const os = require('os');
                                    cookieTmpFile = path.join(os.tmpdir(), `fb_cookies_${account.id}.txt`);
                                    const cookiePairs = account.cookie.replace(/[\r\n]+/g, ' ').trim().split(/;\s*/);
                                    let cookieContent = '# Netscape HTTP Cookie File\n';
                                    for (const pair of cookiePairs) {
                                        const [name, ...valParts] = pair.split('=');
                                        if (name && valParts.length > 0) cookieContent += `.facebook.com\tTRUE\t/\tTRUE\t0\t${name.trim()}\t${valParts.join('=').trim()}\n`;
                                    }
                                    fs.writeFileSync(cookieTmpFile, cookieContent);
                                    ytdlpCookieArgs = ['--cookies', cookieTmpFile];
                                } catch (e) { }
                            }

                            const videoEntries = allVideoIds.map(vid => ({ id: vid, url: `https://www.facebook.com/watch/?v=${vid}` }));
                            await processInBatches(videoEntries, 5, async (entry, i) => {
                                try {
                                    const info = await extractInfo(entry.url, ytdlpCookieArgs);
                                    return { entry, info, i };
                                } catch (e) {
                                    try { return { entry, info: await extractInfo(`https://www.facebook.com/video/${entry.id}`, ytdlpCookieArgs), i }; }
                                    catch (e2) { console.error(`  [Facebook Video Error] ${entry.id}: ${e.message.substring(0, 80)}`); return null; }
                                }
                            }, async (batchResults, currentCount, totalCount) => {
                                for (const resItem of batchResults) {
                                    if (!resItem) continue;
                                    const { entry, info, i } = resItem;
                                    if (info.channel_follower_count && info.channel_follower_count > pageFollowers) pageFollowers = info.channel_follower_count;
                                    if (info.uploader_follower_count && info.uploader_follower_count > pageFollowers) pageFollowers = info.uploader_follower_count;

                                    const views = info.view_count || 0, likes = info.like_count || 0, comments = info.comment_count || 0, shares = info.repost_count || 0;
                                    totalViews += views; totalLikes += likes; totalComments += comments; totalShares += shares;

                                    if (info.uploader && (!pageName || pageName === pageId)) pageName = info.uploader;
                                    if (!avatarUrl && info.thumbnails?.length > 0) avatarUrl = info.thumbnails[info.thumbnails.length - 1].url;

                                    recentContent.push({
                                        id: info.id || entry.id, title: info.title || info.description?.slice(0, 100) || `Facebook Video #${i + 1}`,
                                        description: (info.description || '').slice(0, 300), url: info.webpage_url || entry.url,
                                        thumbnail: info.thumbnail || (info.thumbnails?.length > 0 ? info.thumbnails[Math.min(info.thumbnails.length - 1, 3)].url : null),
                                        views, likes, comments, shares,
                                        duration: info.duration ? Math.round(info.duration) : null,
                                        durationStr: info.duration_string || (info.duration ? `${Math.floor(info.duration / 60)}:${String(Math.round(info.duration % 60)).padStart(2, '0')}` : null),
                                        uploadDate: info.upload_date || null, timestamp: info.timestamp || null, type: 'video'
                                    });
                                }
                                account.metrics = { avatar: avatarUrl, followers: pageFollowers, pageLikes, pageName, pageCategory, totalRecentViews: totalViews, totalRecentLikes: totalLikes, totalRecentComments: totalComments, totalShares, engagementRate: pageFollowers > 0 ? parseFloat(((totalLikes + totalComments) / pageFollowers * 100).toFixed(2)) : 0, videoCount: recentContent.length };
                                account.recentContent = recentContent;
                                saveDB(db);
                                res.write(JSON.stringify({ status: 'update', progress: 30 + Math.round((currentCount / totalCount) * 65) }) + '\n');
                            });

                            if (cookieTmpFile) try { fs.unlinkSync(cookieTmpFile); } catch (e) { }
                        }

                        // Final metrics
                        metrics = { avatar: avatarUrl, followers: pageFollowers, pageLikes, pageName, pageCategory, totalRecentViews: totalViews, totalRecentLikes: totalLikes, totalRecentComments: totalComments, totalShares, engagementRate: pageFollowers > 0 ? parseFloat(((totalLikes + totalComments) / pageFollowers * 100).toFixed(2)) : 0, videoCount: recentContent.length };
                        account.metrics = metrics;
                        account.recentContent = recentContent;
                        saveDB(db);
                        console.log(`  [Facebook] Puppeteer done: name="${pageName}", ${fmt(pageFollowers)} followers, ${recentContent.length} videos`);

                    } catch (puppeteerErr) {
                        if (browser) try { await browser.close(); } catch (e) { }
                        console.log(`  [Facebook] Puppeteer failed: ${puppeteerErr.message}`);
                        metrics = { error: `Não foi possível extrair dados do Facebook. ${fbToken ? 'Token expirado e Puppeteer falhou.' : 'Configure o token da API ou verifique se o Chrome está instalado.'}` };
                    }
                }

            } catch (e) {
                metrics = { error: e.message || 'Erro desconhecido no Facebook' };
            }

        } else if (platform === 'twitter') {
            try {
                console.log(`  [Twitter] Using FxTwitter API...`);

                // Strategy 1: FxTwitter API - free, reliable, no auth needed
                const fxUrl = `https://api.fxtwitter.com/${account.handle}`;
                const fxRes = await fetch(fxUrl, {
                    headers: { 'User-Agent': 'SocialTracker/1.0' }
                });
                const fxData = await fxRes.json();

                if (fxData.code !== 200 || !fxData.user) {
                    throw new Error(`FxTwitter: ${fxData.message || 'Perfil não encontrado'}`);
                }

                const user = fxData.user;
                const followers = user.followers || 0;
                const postCount = user.tweets || 0;
                const avatarUrl = user.avatar_url || null;
                const bannerUrl = user.banner_url || null;

                console.log(`  [Twitter] FxTwitter: ${followers} followers, ${postCount} tweets, avatar OK`);

                // Strategy 2: Syndication API for recent tweet content/metrics
                let totalLikes = 0, totalComments = 0, totalViews = 0;
                try {
                    const synd_url = `https://syndication.twitter.com/srv/timeline-profile/screen-name/${account.handle}`;
                    const html = await fetch(synd_url, {
                        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36' }
                    }).then(r => r.text());

                    const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">(.*?)<\/script>/);
                    if (match) {
                        const data = JSON.parse(match[1]);
                        const timeline = data?.props?.pageProps?.timeline?.entries || [];

                        for (const entry of timeline) {
                            const tweet = entry.content?.tweet;
                            if (!tweet) continue;

                            const views = parseInt(tweet.ext_views?.count || '0');
                            const likes = tweet.favorite_count || 0;
                            const comments = tweet.reply_count || 0;
                            const shares = tweet.retweet_count || 0;

                            totalLikes += likes;
                            totalComments += comments;
                            totalViews += views;

                            recentContent.push({
                                id: tweet.id_str,
                                title: (tweet.text || 'Tweet').slice(0, 100),
                                url: `https://x.com/${account.handle}/status/${tweet.id_str}`,
                                thumbnail: tweet.entities?.media?.[0]?.media_url_https || null,
                                views, likes, comments, shares,
                                uploadDate: new Date(tweet.created_at).toISOString()
                            });
                            if (recentContent.length >= 15) break;
                        }
                        console.log(`  [Twitter] Syndication: ${recentContent.length} tweets`);
                    }
                } catch (e) {
                    console.log(`  [Twitter] Syndication failed (profile data still OK): ${e.message}`);
                }

                metrics = {
                    avatar: avatarUrl,
                    banner: bannerUrl,
                    followers,
                    postCount,
                    totalRecentViews: totalViews,
                    totalRecentLikes: totalLikes,
                    totalRecentComments: totalComments,
                    engagementRate: totalViews > 0 ? parseFloat(((totalLikes + totalComments) / totalViews * 100).toFixed(2)) : 0
                };

            } catch (e) {
                metrics = { error: 'Perfil não encontrado ou privado.', message: e.message };
            }
        }

        // Update account in DB (only if platform didn't already stream/update its own data)
        if (platform !== 'youtube' && platform !== 'tiktok' && platform !== 'facebook') {
            account.metrics = metrics;
            account.recentContent = recentContent;
        }

        account.lastFetch = new Date().toISOString();
        saveDB(db);

        console.log(`[Fetch] Done: ${platform}/@${account.handle} — ${account.recentContent?.length || 0} items`);
        res.end(JSON.stringify({ status: 'done', metrics: account.metrics, recentContent: account.recentContent, lastFetch: account.lastFetch }) + '\n');

    } catch (err) {
        console.error(`[Fetch Error] ${platform}/@${account.handle}:`, err.message);
        account.metrics = { error: err.message };
        saveDB(db);
        if (!res.headersSent) {
            res.status(500).json({ error: err.message });
        } else {
            res.end(JSON.stringify({ error: err.message }) + '\n');
        }
    }
});

// POST fetch ALL accounts for a platform
app.post('/api/fetch-all/:platform', async (req, res) => {
    const { platform } = req.params;
    const db = loadDB();
    const accounts = db[platform] || [];

    if (accounts.length === 0) return res.json({ message: 'No accounts to fetch' });

    const results = [];
    for (const account of accounts) {
        try {
            console.log(`[Fetch-All] ${platform}/@${account.handle}...`);
            const response = await fetch(`http://localhost:${PORT}/api/fetch/${platform}/${account.id}`, { method: 'POST' });
            const data = await response.json();
            results.push({ id: account.id, handle: account.handle, ...data });
        } catch (e) {
            results.push({ id: account.id, handle: account.handle, error: e.message });
        }
    }

    res.json(results);
});

// Quick channel info (for adding accounts — verifies the channel exists)
app.post('/api/verify/:platform', async (req, res) => {
    const { platform } = req.params;
    const { handle } = req.body;

    if (!handle) return res.status(400).json({ error: 'Handle required' });

    const url = buildUrl(platform, handle);

    try {
        const args = [
            '--dump-json',
            '--no-download',
            '--no-warnings',
            '--playlist-end', '1',
            '--flat-playlist',
            url
        ];
        const output = await runYtDlp(args, 30000);
        const info = JSON.parse(output.trim().split('\n')[0]);

        res.json({
            valid: true,
            name: info.uploader || info.channel || handle,
            url
        });
    } catch (e) {
        res.json({ valid: false, error: e.message });
    }
});

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n╔══════════════════════════════════════════╗`);
    console.log(`║  Social Tracker Server running!          ║`);
    console.log(`║  http://localhost:${PORT}                    ║`);
    console.log(`║  yt-dlp backend ready                    ║`);
    console.log(`╚══════════════════════════════════════════╝\n`);
});



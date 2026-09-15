import http from 'node:http';
import {sameHistory,preserveHistory} from './backend/period-history.mjs';
import {Facebook,collectFacebook,facebookId} from './backend/facebook.mjs';
const accountIdValid=id=>/^(?:facebook:)?\d{1,30}$/.test(id);
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {randomBytes,createHash,timingSafeEqual} from 'node:crypto';
import {PersonalStore} from './backend/personal-store.mjs';
import {mediaDetails} from './backend/media-details.mjs';
import {todayViews,todayRange,todayPayload} from './backend/today-views.mjs';
import {Store,encryptionKey} from './backend/store.mjs';
import {Instagram,ProviderError,collect,aggregate,number} from './backend/instagram.mjs';
const publicDir=fileURLToPath(new URL('./public/',import.meta.url));
const assets={'/period-cache.mjs':['period-cache.mjs','text/javascript'],'/facebook-guide':['../FACEBOOK.md','text/markdown'],'/today-views.mjs':['today-views.mjs','text/javascript'],'/media-details.mjs':['media-details.mjs','text/javascript'],'/presentation.mjs':['presentation.mjs','text/javascript'],'/favicon.ico':['favicon.svg','image/svg+xml'],'/favicon.svg':['favicon.svg','image/svg+xml'],'/integration-guide':['../INSTAGRAM_API.md','text/markdown'],'/embed':['embed.html','text/html'],'/embed.mjs':['embed.mjs','text/javascript'],'/':['index.html','text/html'],'/styles.css':['styles.css','text/css'],'/app.mjs':['app.mjs','text/javascript'],'/demo.mjs':['demo.mjs','text/javascript'],'/privacy':['privacy.html','text/html'],'/data-deletion':['data-deletion.html','text/html']};
const hash=x=>createHash('sha256').update(x).digest('hex');
const opaque=()=>randomBytes(32).toString('base64url');
const COOKIE='__Host-lume_session',SESSION_AGE=180*86400000;
export function createServer(env=process.env,options={}){
 const personal=Boolean(env.PERSONAL_PASSWORD);
 const now=options.now||Date.now,key=encryptionKey(env.TOKEN_ENCRYPTION_KEY),missingConfiguration=[];
 for(const k of ['META_APP_ID','META_APP_SECRET','PUBLIC_BASE_URL','DATA_DIR'])if(!env[k])missingConfiguration.push(k);
 if(!key)missingConfiguration.push('TOKEN_ENCRYPTION_KEY');
 let origin;try{const u=new URL(env.PUBLIC_BASE_URL);if(u.protocol!=='https:'||u.username||u.password||u.pathname!=='/'||u.search||u.hash||env.PUBLIC_BASE_URL.endsWith('/'))throw Error();origin=u.origin;}catch{if(!missingConfiguration.includes('PUBLIC_BASE_URL'))missingConfiguration.push('PUBLIC_BASE_URL');}
 if(env.API_VERSION&&!/^v\d+\.0$/.test(env.API_VERSION))missingConfiguration.push('API_VERSION');
 const sharedMissing=missingConfiguration.filter(k=>!['META_APP_ID','META_APP_SECRET','API_VERSION'].includes(k));
 const facebookMissing=[...sharedMissing];for(const k of ['FACEBOOK_APP_ID','FACEBOOK_APP_SECRET'])if(!env[k])facebookMissing.push(k);if(env.FACEBOOK_API_VERSION&&!/^v\d+\.0$/.test(env.FACEBOOK_API_VERSION))facebookMissing.push('FACEBOOK_API_VERSION');
 let store,storageError=false;if(!sharedMissing.length){try{store=personal?new PersonalStore(env.DATA_DIR,key):new Store(env.DATA_DIR,key);}catch{storageError=true;}}
 const configured=Boolean(store)&&!missingConfiguration.length,facebookConfigured=Boolean(store)&&!facebookMissing.length,fb=new Facebook(env,options.fetch||fetch),api=new Instagram(env,options.fetch||fetch),rates=new Map(),flights=new Map();
 const generations=new WeakMap();let generation=0;const connectionKey=c=>{if(!generations.has(c))generations.set(c,++generation);return generations.get(c);};
 const validHistory=(c,days)=>(c.provider!=='facebook'||c.cache?.[days]?.data.bucketQueryVersion===2)&&sameHistory(c,c.cache?.[days],days,now())&&now()-c.cache[days].savedAt<300000;
 function limited(key,limit,window){const t=now();for(const [k,v]of rates)if(v.until<t)rates.delete(k);let r=rates.get(key);if(!r){if(rates.size>10000)return true;r={count:0,until:t+window};rates.set(key,r);}return ++r.count>limit;}
 function session(req){const raw=req.headers.cookie?.split(';').map(x=>x.trim()).find(x=>x.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);if(!raw||!/^[-_A-Za-z0-9]{43}$/.test(raw)||!store)return null;const id=hash(raw),s=store.data.sessions[id];return s&&s.expiresAt>now()?{id,s}:null;}
 function createSession(res){for(const[id,s]of Object.entries(store.data.sessions))if(s.expiresAt<=now())delete store.data.sessions[id];if(Object.keys(store.data.sessions).length>=10000)throw Error('capacity');const raw=opaque(),id=hash(raw),s={expiresAt:now()+SESSION_AGE,connections:{},states:{}};store.data.sessions[id]=s;res.setHeader('Set-Cookie',`${COOKIE}=${raw}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_AGE/1000}`);return {id,s};}
 function publicAccount(c){return {id:c.id,provider:c.provider||'instagram',providerId:c.providerId||c.id,accountType:c.provider==='facebook'?'page':'professional',username:c.profile.username||'',name:c.profile.name||c.profile.username||'',followers:number(c.profile.followers_count),following:number(c.profile.follows_count),mediaCount:number(c.profile.media_count),status:c.expiresAt<=now()||c.status==='expired'?'expired':c.status||'connected',lastSynced:c.lastSynced||null,tokenLifetime:c.tokenLifetime||'long',connectedAt:c.tokenIssuedAt?new Date(c.tokenIssuedAt).toISOString():null,expiresAt:new Date(c.expiresAt).toISOString()};}
 const server=http.createServer(async(req,res)=>{
  res.setHeader('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; frame-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
  res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('X-Frame-Options','DENY');res.setHeader('Referrer-Policy','no-referrer');res.setHeader('Permissions-Policy','camera=(), microphone=(), geolocation=()');res.setHeader('Cache-Control','no-store');
  const send=(status,data)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(req.method==='HEAD'?undefined:JSON.stringify(data));};
  const redirect=location=>{res.writeHead(302,{Location:location});res.end();};
  let url;try{url=new URL(req.url,'http://localhost');}catch{return send(400,{error:'invalid_request'});}
  try{
   const path=url.pathname,sess=session(req);
   const providerFilter=url.searchParams.get('provider')||'all';
   if(['/api/dashboard','/api/sync','/api/today-views'].includes(path)&&!['all','instagram','facebook'].includes(providerFilter))return send(400,{error:'invalid_provider_filter'});
   if(personal&&path!=='/healthz'){
    const supplied=Buffer.from(req.headers.authorization||''),expected=Buffer.from('Basic '+Buffer.from((env.PERSONAL_USERNAME||'miguel')+':'+env.PERSONAL_PASSWORD).toString('base64'));
    if(supplied.length!==expected.length||!timingSafeEqual(supplied,expected)){
     if(limited('access:'+req.socket.remoteAddress,100,600000))return send(429,{error:'access_rate_limited'});
     res.setHeader('WWW-Authenticate','Basic realm="Lume pessoal", charset="UTF-8"');return send(401,{error:'personal_login_required'});
    }
   }
   if(path==='/api/diagnostics'&&personal)return send(200,{lastOAuthError:store?.data.workspace.lastOAuthError||null});
   if(path==='/api/accounts'&&personal){if(req.method!=='GET')return send(405,{error:'method_not_allowed'});return send(200,{accounts:store?Object.values(store.data.workspace.connections).map(publicAccount):[]});}
   if(path==='/healthz')return send(200,{ok:true});
   if(path==='/api/config'){if(req.method!=='GET')return send(405,{error:'method_not_allowed'});return send(200,{configured,provider:'instagram',providers:{instagram:{configured,missingConfiguration},facebook:{configured:facebookConfigured,missingConfiguration:facebookMissing,apiVersion:fb.version}},connected:personal?Boolean(store&&Object.keys(store.data.workspace.connections).length):Boolean(sess&&Object.keys(sess.s.connections).length),accountCount:personal&&store?Object.keys(store.data.workspace.connections).length:undefined,oauthImplemented:true,sessionOwnership:personal?'personal':'browser',missingConfiguration,storageAvailable:!storageError});}
   if(path==='/api/auth/start'){
    if(req.method!=='GET')return send(405,{error:'method_not_allowed'});
    const provider=url.searchParams.get('provider')||'instagram';
    if(!['instagram','facebook'].includes(provider))return send(400,{error:'unsupported_provider'});
    if(provider==='facebook'?!facebookConfigured:!configured)return send(503,{connected:false,error:storageError?'storage_unavailable':'integration_not_configured'});
    // Ignore spoofable forwarded-for; reverse proxy deployments share this generous IP bucket.
    if(limited('oauth:'+req.socket.remoteAddress,10,600000))return send(429,{error:'oauth_rate_limited'});
    const owner=sess||createSession(res);if(limited('session:'+owner.id,8,600000))return send(429,{error:'oauth_rate_limited'});
    const state=opaque();for(const[k,t]of Object.entries(owner.s.states))if(t<now())delete owner.s.states[k];owner.s.states[(provider==='facebook'?'facebook:':'')+hash(state)]=now()+600000;store.save();
    if(provider==='facebook')return redirect(fb.authorize(state));
    const u=new URL('https://www.instagram.com/oauth/authorize');u.search=new URLSearchParams({client_id:env.META_APP_ID,redirect_uri:env.PUBLIC_BASE_URL+'/auth/instagram/callback',response_type:'code',scope:'instagram_business_basic,instagram_business_manage_insights',state,enable_fb_login:'false',force_reauth:'false'});return redirect(u.href);
   }
   if(path==='/auth/facebook/callback'){
    if(req.method!=='GET')return send(405,{error:'method_not_allowed'});
    const reject=(code,stage='callback_validation')=>{if(personal&&store){store.data.workspace.lastOAuthError={code,stage,provider:'facebook',at:new Date(now()).toISOString()};store.save();}return redirect('/?error='+code+'&provider=facebook');};
    const state=url.searchParams.get('state');
    if(!facebookConfigured||!sess||!state||!/^[-_A-Za-z0-9]{43}$/.test(state))return reject('invalid_state');
    const stateId='facebook:'+hash(state),expires=sess.s.states[stateId];delete sess.s.states[stateId];store.save();
    if(!expires||expires<=now())return reject('invalid_state');
    if(limited('callback:'+req.socket.remoteAddress,20,600000))return reject('oauth_rate_limited');
    if(url.searchParams.has('error'))return reject('authorization_denied');
    const code=url.searchParams.get('code');if(!code||code.length>4096)return reject('invalid_code');
    let stage='facebook_token_exchange';
    try{
     const client=Object.assign(new Facebook(env,options.fetch||fetch),{budget:10,signal:AbortSignal.timeout(20000)});
     const userToken=await client.exchange(code);stage='facebook_page_discovery';const pages=await client.discover(userToken.access_token);
     const saved=personal?store.data.workspace.connections:sess.s.connections;
     if(new Set([...Object.keys(saved),...pages.map(p=>facebookId(p.id))]).size>10)throw new ProviderError('account_limit');
     // All discovered/authorized ANALYZE Pages connect atomically; no user token is persisted.
     for(const page of pages){const id=facebookId(page.id);saved[id]={id,provider:'facebook',providerId:page.id,profile:{id:page.id,name:page.name},token:page.access_token,tokenLifetime:'page',tokenIssuedAt:now(),expiresAt:now()+Math.min(userToken.expires_in,60*86400)*1000,expiryBasis:'local_reauthorization_deadline',status:'connected',cache:{}};}
     if(personal)store.data.workspace.lastOAuthError=null;store.save();return redirect('/?connected=1&provider=facebook');
    }catch(e){return reject(e instanceof ProviderError?e.code:'connection_failed',stage);}
   }
   if(path==='/auth/instagram/callback'){
    if(req.method!=='GET')return send(405,{error:'method_not_allowed'});
    const reject=(code)=>{if(personal&&store){store.data.workspace.lastOAuthError={code,stage:'callback_validation',at:new Date(now()).toISOString()};store.save();}return redirect('/?error='+code);};
    const state=url.searchParams.get('state');if(!configured||!sess||!state||!/^[-_A-Za-z0-9]{43}$/.test(state))return reject('invalid_state');
    const stateId=hash(state),expires=sess.s.states[stateId];delete sess.s.states[stateId];store.save();if(!expires||expires<=now())return reject('invalid_state');
    if(limited('callback:'+req.socket.remoteAddress,20,600000))return reject('oauth_rate_limited');
    if(url.searchParams.has('error'))return reject('authorization_denied');
    const code=url.searchParams.get('code');if(!code||code.length>4096)return reject('invalid_code');
    let stage='token_exchange';try{const token=await api.exchange(code);stage='profile';const rawProfile=await api.graph('me',token.access_token,{fields:'user_id,username'}),profile=rawProfile.data?.[0]||rawProfile;const id=String(profile.user_id||profile.id||'');if(!/^\d+$/.test(id)||typeof profile.username!=='string')throw new ProviderError('invalid_profile');const saved=personal?store.data.workspace.connections:sess.s.connections;if(Object.keys(saved).length>=10&&!saved[id])throw new ProviderError('account_limit');saved[id]={id,profile,token:token.access_token,tokenLifetime:token.tokenLifetime||'long',upgradeAttemptAt:token.tokenLifetime==='short'?now():undefined,tokenIssuedAt:now(),expiresAt:now()+token.expires_in*1000,status:'connected',cache:{}};if(personal){store.data.workspace.lastOAuthError=token.upgradeError?{code:token.upgradeError.code,stage:'long_token_upgrade',at:new Date(now()).toISOString(),provider:token.upgradeError.details}:null;}store.save();return redirect('/?connected=1'+(token.tokenLifetime==='short'?'&warning=short_lived_token':''));}catch(e){const failure={code:e instanceof ProviderError?e.code:'connection_failed',stage,at:new Date(now()).toISOString()};if(personal){store.data.workspace.lastOAuthError={...failure,...(e instanceof ProviderError&&e.details?{provider:e.details}:{})};store.save();}console.warn(JSON.stringify({event:'oauth_failed',...failure}));return redirect('/?error='+failure.code);}
   }
   if(path==='/api/today-views'){
    if(req.method!=='GET')return send(405,{error:'method_not_allowed'});
    const owner=personal&&store?{id:'personal',s:store.data.workspace}:sess;
    if(!owner)return send(401,{error:'session_required'});
    const account=url.searchParams.get('account')||'all';
    if(!(account==='all'||accountIdValid(account)))return send(400,{error:'invalid_filter'});
    const connections=owner.s.connections;
    if(account!=='all'&&!Object.hasOwn(connections,account))return send(404,{error:'account_not_found'});
    const selected=Object.values(connections).filter(c=>(account==='all'||account===c.id)&&(providerFilter==='all'||(c.provider||'instagram')===providerFilter));
    if(selected.length>10)return send(400,{error:'account_limit'});
    if(limited('today-read:'+owner.id,60,60000))return send(429,{error:'today_rate_limited'});
    const timestamp=now(),day=todayRange(timestamp).since;
    const force=url.searchParams.get('refresh')==='1';
    const valid=c=>(!force||timestamp-c.todayViewsCache?.savedAt<30000)&&c.todayViewsCache&&c.todayViewsCache.data.error!=='facebook_today_unsupported'&&c.todayViewsCache.data.range.since===day&&timestamp-c.todayViewsCache.savedAt<300000&&c.expiresAt>timestamp;
    const flightKey=c=>owner.id+':today:'+c.id+':'+connectionKey(c)+':'+day;
    const needed=selected.filter(c=>!valid(c)&&!flights.has(flightKey(c)));
    if(needed.length&&limited('today-provider:'+owner.id,6,60000))return send(429,{error:'today_rate_limited'});
    const results=await Promise.all(selected.map(async c=>{
     if(valid(c))return c.todayViewsCache.data;
     const key=flightKey(c);let p=flights.get(key);
     if(!p){p=todayViews(api,c,timestamp);flights.set(key,p);}
     let data;try{data=await p;}finally{if(flights.get(key)===p)flights.delete(key);}
     if(connections[c.id]===c)c.todayViewsCache={savedAt:now(),data};
     return data;
    }));
    if(selected.some(c=>connections[c.id]!==c))return send(409,{error:'connection_changed'});
    if(needed.length)store.save();
    return send(200,{...todayPayload(results,Object.values(connections).map(publicAccount),timestamp),selectedAccountIds:selected.map(c=>c.id)});
   }
   if(path==='/api/media-details'){
    if(req.method!=='GET')return send(405,{error:'method_not_allowed'});
    const owner=personal&&store?{id:'personal',s:store.data.workspace}:sess;
    if(!owner)return send(401,{error:'session_required'});
    const account=url.searchParams.get('account')||'',mediaId=url.searchParams.get('media')||'';
    if(!accountIdValid(account)||!(/^(?:facebook:)?\d{1,30}(?:_\d{1,30})?$/.test(mediaId)))return send(400,{error:'invalid_media_filter'});
    const c=Object.hasOwn(owner.s.connections,account)?owner.s.connections[account]:null;
    if(!c)return send(404,{error:'account_not_found'});
    if(c.provider==='facebook')return send(422,{error:'facebook_media_details_unsupported',provider:'facebook',message:'Detalhes avançados do Facebook indisponíveis. Consulte as métricas acumuladas na tabela.'});
    const media=Object.values(c.cache||{}).sort((a,b)=>b.savedAt-a.savedAt).flatMap(x=>x.data?.content||[]).find(x=>x.id===mediaId&&x.accountId===account);
    if(!media)return send(404,{error:'media_not_found'});
    if(limited('media-read:'+owner.id,60,60000))return send(429,{error:'media_rate_limited'});
    const cached=c.mediaDetailsCache?.[mediaId];
    if(cached&&now()-cached.savedAt<300000&&c.expiresAt>now())return send(200,cached.data);
    const mediaFlightGeneration=connectionKey(c);
    const flightKey=owner.id+':media:'+account+':'+mediaFlightGeneration+':'+mediaId;
    let p=flights.get(flightKey);
    if(!p){
     if(limited('media-provider:'+owner.id,12,60000))return send(429,{error:'media_rate_limited'});
     p=mediaDetails(api,c,media,now());flights.set(flightKey,p);
    }
    let data;try{data=await p;}finally{if(flights.get(flightKey)===p)flights.delete(flightKey);}
    if(owner.s.connections[account]!==c||connectionKey(c)!==mediaFlightGeneration)return send(409,{error:'connection_changed'});
    c.mediaDetailsCache||={};
    for(const [id,v]of Object.entries(c.mediaDetailsCache))if(now()-v.savedAt>=300000)delete c.mediaDetailsCache[id];
    if(!c.mediaDetailsCache[mediaId]&&Object.keys(c.mediaDetailsCache).length>=36)delete c.mediaDetailsCache[Object.keys(c.mediaDetailsCache).sort((a,b)=>c.mediaDetailsCache[a].savedAt-c.mediaDetailsCache[b].savedAt)[0]];
    c.mediaDetailsCache[mediaId]={savedAt:now(),data};store.save();return send(200,data);
   }
   if(['/api/dashboard','/api/sync','/api/disconnect'].includes(path)){
    const owner=personal&&store?{id:'personal',s:store.data.workspace}:sess;
    const mutating=path!=='/api/dashboard';if(req.method!==(mutating?'POST':'GET'))return send(405,{error:'method_not_allowed'});
    if(mutating&&(!origin||req.headers.origin!==origin||req.headers['sec-fetch-site']==='cross-site'))return send(403,{error:'csrf_rejected'});
    const account=url.searchParams.get('account')||'all',days=Number(url.searchParams.get('days')||30);
    if(![1,7,30,90].includes(days)||!(account==='all'||accountIdValid(account)))return send(400,{error:'invalid_filter'});
    const connections=owner?owner.s.connections:{};if(account!=='all'&&!Object.hasOwn(connections,account))return send(404,{error:'account_not_found'});
    if(path==='/api/disconnect'){if(!owner)return send(401,{error:'session_required'});for(const id of Object.keys(connections))if(account==='all'||account===id)delete connections[id];store.save();return send(200,{ok:true,connected:Boolean(Object.keys(connections).length)});}
    const selected=Object.values(connections).filter(c=>(account==='all'||account===c.id)&&(providerFilter==='all'||(c.provider||'instagram')===providerFilter));
    if(!selected.length)return send(200,{mode:'live',connected:Boolean(Object.keys(connections).length),accounts:Object.values(connections).map(publicAccount),selectedAccountIds:[],content:[],metrics:null,series:[],errors:[],lastSynced:null,range:{days,timezone:'UTC'},message:'Nenhuma conta conectada. Dados reais indisponíveis.'});
    if(limited('dashboard:'+owner.id,30,60000))return send(429,{error:'sync_rate_limited'});
    const needsSync=selected.some(c=>path==='/api/sync'||!validHistory(c,days));
    if(needsSync){const inFlight=selected.every(c=>flights.has(owner.id+':'+c.id+':'+connectionKey(c)+':'+days));if(!inFlight&&path==='/api/sync'&&owner.s.nextSyncAt>now())return send(429,{error:'sync_cooldown'});if(!inFlight){owner.s.nextSyncAt=now()+30000;store.save();}}
    const selectedGenerations=selected.map(connectionKey);
    const results=await Promise.all(selected.map(async c=>{const cache=c.cache?.[days];if(validHistory(c,days)&&path!=='/api/sync')return cache.data;const flightKey=owner.id+':'+c.id+':'+connectionKey(c)+':'+days;if(flights.has(flightKey))return flights.get(flightKey);const p=(async()=>{const fresh=await (c.provider==='facebook'?collectFacebook(fb,c,days,now()):collect(api,c,days,now()));if(connections[c.id]!==c||flightKey!==owner.id+':'+c.id+':'+connectionKey(c)+':'+days)return fresh;const revoked=fresh.errors.some(e=>['token_expired','facebook_permissions_missing'].includes(e.code));const data=preserveHistory(c,cache,fresh,days,now());if(revoked){if(c.status!=='expired'&&c.expiresAt>now())generations.set(c,++generation);c.cache={};delete c.mediaDetailsCache;delete c.todayViewsCache;}c.status=revoked?'expired':data.errors.some(e=>c.provider!=='facebook'||!['facebook_reach_unsupported','metric_unavailable','incomplete_daily_series','content_truncated'].includes(e.code))?'error':'connected';c.lastSynced=data.lastSynced;c.cache||={};c.cache[days]={savedAt:now(),data};store.save();return data;})();flights.set(flightKey,p);try{return await p;}finally{flights.delete(flightKey);}}));
    // A simultaneous disconnect must not return a detached connection's data.
    if(selected.some((c,i)=>connections[c.id]!==c||selectedGenerations[i]!==connectionKey(c)))return send(409,{error:'connection_changed'});
    return send(200,{mode:'live',connected:true,accounts:Object.values(connections).map(publicAccount),selectedAccountIds:selected.map(c=>c.id),stale:results.some(r=>r.stale),refreshAttemptedAt:results.map(r=>r.refreshAttemptedAt).filter(Boolean).sort().at(-1)||null,accountMetrics:results.map((r,i)=>({accountId:selected[i].id,provider:selected[i].provider||'instagram',metrics:r.metrics,stale:!!r.stale,lastSynced:r.lastSynced,refreshAttemptedAt:r.refreshAttemptedAt||null})),accountRanges:results.map((r,i)=>({accountId:selected[i].id,range:r.range})),content:results.flatMap(r=>r.content).sort((a,b)=>b.timestamp.localeCompare(a.timestamp)),metrics:aggregate(results),series:results.flatMap(r=>r.series),errors:results.flatMap(r=>r.errors),partial:results.some(r=>r.errors.length),truncated:results.some(r=>r.truncated),range:results[0].range,lastSynced:results.map(r=>r.lastSynced).sort()[0],source:selected.every(c=>c.provider==='facebook')?'Facebook Pages API':selected.every(c=>c.provider!=='facebook')?'Instagram API':'Instagram + Facebook Pages APIs',metricsScope:selected.some(c=>c.provider==='facebook')?'provider_daily_buckets_and_lifetime_posts':undefined,engagementFormula:'interactions / reach * 100',message:results.some(r=>r.errors.length)?'Algumas métricas estão indisponíveis.':null});
   }
   if(!['GET','HEAD'].includes(req.method))return send(405,{error:'method_not_allowed'});
   if(path.startsWith('/api/')||path.startsWith('/auth/'))return send(404,{error:'not_found'});
   if(path==='/embed'){res.removeHeader('X-Frame-Options');res.setHeader('Content-Security-Policy',String(res.getHeader('Content-Security-Policy')).replace("frame-ancestors 'none'","frame-ancestors *"));}
   const asset=assets[path];if(!asset)return send(404,{error:'not_found'});const body=await readFile(resolve(publicDir,asset[0]));res.writeHead(200,{'Content-Type':asset[1]+'; charset=utf-8'});res.end(req.method==='HEAD'?undefined:body);
  }catch{return send(500,{error:'internal_error'});}
 });
 server.on('close',()=>store?.close?.());
 server.requestTimeout=30000;server.headersTimeout=15000;return server;
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){const port=Number(process.env.PORT||5066),host=process.env.HOST||'127.0.0.1';createServer().listen(port,host,()=>console.log(`Lume listening on http://${host}:${port}`));}

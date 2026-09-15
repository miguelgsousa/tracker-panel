export const number=v=>typeof v==='number'&&Number.isFinite(v)?v:null;
export class ProviderError extends Error{constructor(code,details){super(code);this.code=code;if(details)this.details=details;}}
function diagnostic(url,init,env,r,d){
 const u=new URL(url),secrets=[env.META_APP_SECRET];
 for(const source of [u.searchParams,init.body instanceof URLSearchParams?init.body:new URLSearchParams()])for(const k of ['code','access_token','client_secret'])secrets.push(source.get(k));
 if(init.headers?.Authorization)secrets.push(init.headers.Authorization.replace(/^Bearer /,''));
 let message=String(d.error?.message||d.error_message||'');
 for(const secret of secrets.filter(Boolean).sort((a,b)=>b.length-a.length))message=message.split(secret).join('[redacted]');
 message=message.replace(/https?:\/\/\S+/g,'[url]').replace(/(?:access_token|client_secret|code)\s*[=:]\s*[^\s,;]+/gi,'[redacted]').replace(/\b[A-Za-z0-9_-]{24,}\b/g,'[redacted]').replace(/[\x00-\x1f]/g,' ').slice(0,500);
 const code=d.error?.code??d.code,subcode=d.error?.error_subcode,type=String(d.error?.type||d.error_type||'');
 return {endpoint:u.pathname==='/oauth/access_token'?'short_token':u.pathname==='/access_token'?'long_token':'graph',httpStatus:r.status,providerCode:Number.isInteger(code)?code:null,providerSubcode:Number.isInteger(subcode)?subcode:null,type:/^[A-Za-z_]{1,60}$/.test(type)?type:null,message};
}
export class Instagram {
 constructor(env,fetcher=fetch){this.env=env;this.fetch=fetcher;this.version=env.API_VERSION||'v25.0';}
 async request(url,init={}){try{if(this.budget!==undefined&&--this.budget<0)throw new ProviderError('sync_budget_exceeded');if(this.signal?.aborted)throw new ProviderError('sync_timeout');const r=await this.fetch(String(url),{...init,redirect:'error',signal:this.signal?AbortSignal.any([this.signal,AbortSignal.timeout(10000)]):AbortSignal.timeout(10000)});const text=await r.text();if(text.length>2000000)throw new ProviderError('provider_response_too_large');const d=JSON.parse(text);if(!r.ok||d.error||d.error_type){const details=diagnostic(String(url),init,this.env,r,d),code=d.error?.code,message=String(d.error?.message||d.error_message||'').toLowerCase();if(/client.secret|invalid.client|invalid app.secret/.test(message))throw new ProviderError('invalid_client',details);if(/redirect_uri|redirect uri/.test(message))throw new ProviderError('invalid_redirect',details);if(/authorization code.*(expired|used|invalid)|code.*already.*used/.test(message))throw new ProviderError('invalid_code',details);throw new ProviderError(code===190?'token_expired':r.status===429||[4,17,32,613].includes(code)?'provider_rate_limited':[10,200].includes(code)?'permission_denied':'provider_error',details);}return d;}catch(e){if(e instanceof ProviderError)throw e;throw new ProviderError(e.name==='TimeoutError'||e.name==='AbortError'?'provider_timeout':'provider_unavailable');}}
 async exchange(code){const body=new URLSearchParams({client_id:this.env.META_APP_ID,client_secret:this.env.META_APP_SECRET,grant_type:'authorization_code',redirect_uri:this.env.PUBLIC_BASE_URL+'/auth/instagram/callback',code});const short=await this.request('https://api.instagram.com/oauth/access_token',{method:'POST',body});const token=short.data?.[0]?.access_token||short.access_token;if(typeof token!=='string')throw new ProviderError('invalid_token_response');try{return await this.upgrade(token);}catch(e){if(!(e instanceof ProviderError))throw e;const ttl=Number(short.data?.[0]?.expires_in??short.expires_in);return {access_token:token,expires_in:ttl>0?Math.min(ttl,1800):1800,tokenLifetime:'short',upgradeError:{code:e.code,details:e.details||null}};}}
 async upgrade(token){const u=new URL('https://graph.instagram.com/access_token');u.search=new URLSearchParams({grant_type:'ig_exchange_token',client_secret:this.env.META_APP_SECRET,access_token:token});const long=await this.request(u,{method:'GET'});if(typeof long.access_token!=='string'||!(long.expires_in>0))throw new ProviderError('invalid_token_response');return {...long,tokenLifetime:'long'};}
 async refresh(token){const u=new URL('https://graph.instagram.com/refresh_access_token');u.search=new URLSearchParams({grant_type:'ig_refresh_token',access_token:token});const d=await this.request(u);if(typeof d.access_token!=='string'||!(d.expires_in>0))throw new ProviderError('invalid_token_response');return d;}
 graph(path,token,params={}){const u=new URL(`https://graph.instagram.com/${this.version}/${path}`);u.search=new URLSearchParams(params);return this.request(u,{headers:{Authorization:`Bearer ${token}`}});}
 async profile(token,id='me'){const d=await this.graph(id,token,{fields:'user_id,username,name,account_type,followers_count,follows_count,media_count,profile_picture_url'});return d.data?.[0]||d;}
}
const sum=xs=>xs.length&&xs.every(x=>number(x)!==null)?xs.reduce((a,b)=>a+b,0):null;
const value=(d,m)=>{const row=d.data?.find(x=>x.name===m);return number(row?.total_value?.value??row?.values?.[0]?.value);};
export async function collect(api,connection,days,now){
 api=Object.assign(new Instagram(api.env,api.fetch),{budget:100,signal:AbortSignal.timeout(20000)});
 const until=Math.floor(now/86400000)*86400,since=until-days*86400,errors=[],series=[],content=[];const metrics={followers:null,reach:null,views:null,interactions:null,accountsEngaged:null,engagementRate:null};
 const attempt=async(metric,fn)=>{try{return await fn();}catch(e){errors.push({accountId:connection.id,metric,code:e instanceof ProviderError?e.code:'provider_error'});return null;}};
 if(connection.status==='expired'||connection.expiresAt<=now){errors.push({accountId:connection.id,code:'token_expired'});return {metrics,series,content,errors,range:{since,until,timezone:'UTC'},lastSynced:new Date(now).toISOString()};}
 if(connection.tokenLifetime==='short'){
  if(!connection.upgradeAttemptAt||now-connection.upgradeAttemptAt>=60000){connection.upgradeAttemptAt=now;const upgraded=await attempt('token_upgrade',()=>api.upgrade(connection.token));if(upgraded){connection.token=upgraded.access_token;connection.tokenLifetime='long';connection.tokenIssuedAt=now;connection.expiresAt=now+upgraded.expires_in*1000;delete connection.upgradeError;}}
  if(connection.tokenLifetime==='short')errors.push({accountId:connection.id,code:'short_lived_token'});
 }
 if(connection.tokenLifetime!=='short'&&connection.expiresAt-now<7*86400000&&now-connection.tokenIssuedAt>86400000){const r=await attempt('token_refresh',()=>api.refresh(connection.token));if(r){connection.token=r.access_token;connection.tokenIssuedAt=now;connection.expiresAt=now+r.expires_in*1000;}}
 const profile=await attempt('profile',()=>api.profile(connection.token,connection.id));if(profile){connection.profile=profile;metrics.followers=number(profile.followers_count);}
 const windows=[];for(let start=since;start<until;start+=30*86400)windows.push([start,Math.min(until,start+30*86400)]);
 for(const [metric,key] of [['reach','reach'],['views','views'],['total_interactions','interactions'],['accounts_engaged','accountsEngaged']]){
  if(windows.length>1&&['reach','accounts_engaged'].includes(metric)){errors.push({accountId:connection.id,metric,code:'unique_metric_range_unavailable'});continue;}
  const parts=await Promise.all(windows.map(async([start,end])=>{const d=await attempt(metric,()=>api.graph(`${connection.id}/insights`,connection.token,{metric,period:'day',metric_type:'total_value',since:String(start),until:String(end-1)}));const v=d?value(d,metric):null;if(d&&v===null)errors.push({accountId:connection.id,metric,code:'metric_unavailable'});return v;}));metrics[key]=sum(parts);
 }
 for(const [start,end] of windows){const d=await attempt('reach_series',()=>api.graph(`${connection.id}/insights`,connection.token,{metric:'reach',period:'day',metric_type:'time_series',since:String(start),until:String(end-1)}));for(const row of d?.data?.find(x=>x.name==='reach')?.values||[]){if(row.end_time&&number(row.value)!==null)series.push({date:row.end_time,reach:row.value,accountId:connection.id});}}
 // Cursor pagination only: never follow a provider-supplied URL with a bearer token.
 let after;const seen=new Set();let truncated=false;for(let page=0;page<5;page++){
 const d=await attempt('media',()=>api.graph(`${connection.id}/media`,connection.token,{fields:'id,caption,media_type,media_product_type,timestamp,permalink,thumbnail_url,media_url',limit:'25',...(after?{after}:{})}));if(!d)break;
 let older=false;for(const m of d.data||[]){if(content.length>=12||api.signal.aborted||api.budget<7){truncated=true;break;}if(!/^\d+$/.test(String(m.id))||seen.has(m.id))continue;seen.add(m.id);const time=Date.parse(m.timestamp);if(time<since*1000){older=true;continue;}if(!Number.isFinite(time)||time>=until*1000)continue;
 const item={id:String(m.id),accountId:connection.id,account:connection.id,caption:m.caption||'',title:m.caption||'Instagram',type:m.media_product_type==='REELS'?'reel':m.media_product_type==='STORY'?'story':m.media_type==='CAROUSEL_ALBUM'?'carousel':'image',date:m.timestamp,timestamp:m.timestamp,url:safeUrl(m.permalink),thumbnail:safeUrl(m.thumbnail_url||m.media_url),reach:null,views:null,likes:null,comments:null,saved:null,shares:null,interactions:null,engagementRate:null,metricsScope:'lifetime'};
 const product=m.media_product_type||'FEED';const compatible=['FEED','REELS'].includes(product)?['reach','views','likes','comments','saved','shares','total_interactions']:product==='STORY'?['reach','views','shares','total_interactions']:[];
 await Promise.all(compatible.map(async metric=>{const result=await attempt(metric,()=>api.graph(`${m.id}/insights`,connection.token,{metric}));const v=result?value(result,metric):null;item[metric==='total_interactions'?'interactions':metric]=v;if(result&&v===null)errors.push({accountId:connection.id,mediaId:String(m.id),metric,code:'metric_unavailable'});}));if(item.reach>0&&item.interactions!==null)item.engagementRate=item.interactions/item.reach*100;content.push(item);
 }
 after=d.paging?.cursors?.after;if(truncated||older||!d.paging?.next||!after)break;if(page===4)truncated=true;
 }
 if(metrics.reach>0&&metrics.interactions!==null)metrics.engagementRate=metrics.interactions/metrics.reach*100;
 if(truncated)errors.push({accountId:connection.id,metric:'media',code:'content_truncated'});
 return {metrics,series,content,errors,truncated,partial:errors.length>0,range:{since,until,timezone:'UTC',days},lastSynced:new Date(now).toISOString()};
}
function safeUrl(v){try{const u=new URL(v);return u.protocol==='https:'?u.href:null;}catch{return null;}}
export function aggregate(results){const metrics={};for(const key of ['followers','views','interactions'])metrics[key]=sum(results.map(r=>r.metrics[key]));metrics.reach=results.length===1?results[0].metrics.reach:null;metrics.accountsEngaged=results.length===1?results[0].metrics.accountsEngaged:null;metrics.engagementRate=metrics.reach>0&&metrics.interactions!==null?metrics.interactions/metrics.reach*100:null;return metrics;}

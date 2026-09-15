import {Instagram,ProviderError,number} from './instagram.mjs';
export const FACEBOOK_SCOPES=['pages_show_list','pages_read_engagement','read_insights'];
const count=v=>number(v)!==null&&v>=0?v:null;
const numeric=v=>typeof v==='string'&&/^\d{1,30}$/.test(v);
export const facebookId=id=>'facebook:'+id;
// Reuse only the hardened HTTP transport, NEVER Instagram auth/token methods.
export class Facebook extends Instagram {
 constructor(env,fetcher=fetch){super({...env,META_APP_SECRET:env.FACEBOOK_APP_SECRET},fetcher);this.version=env.FACEBOOK_API_VERSION||'v26.0';}
 async request(url,init={}){try{return await super.request(url,init);}catch(e){if(e instanceof ProviderError&&e.details){const {httpStatus,providerCode,providerSubcode,type}=e.details;e.details={httpStatus,providerCode,providerSubcode,type};}throw e;}}
 authorize(state){const u=new URL(`https://www.facebook.com/${this.version}/dialog/oauth`);u.search=new URLSearchParams({client_id:this.env.FACEBOOK_APP_ID,redirect_uri:this.env.PUBLIC_BASE_URL+'/auth/facebook/callback',response_type:'code',scope:FACEBOOK_SCOPES.join(','),state});return u.href;}
 graph(path,token,params={}){const u=new URL(`https://graph.facebook.com/${this.version}/${path}`);u.search=new URLSearchParams(params);return this.request(u,{headers:{Authorization:`Bearer ${token}`}});}
 async exchange(code){
  const request=async params=>{const u=new URL(`https://graph.facebook.com/${this.version}/oauth/access_token`);u.search=new URLSearchParams({client_id:this.env.FACEBOOK_APP_ID,client_secret:this.env.FACEBOOK_APP_SECRET,...params});return this.request(u);};
  const short=await request({redirect_uri:this.env.PUBLIC_BASE_URL+'/auth/facebook/callback',code});
  if(typeof short.access_token!=='string'||!short.access_token)throw new ProviderError('invalid_token_response');
  const long=await request({grant_type:'fb_exchange_token',fb_exchange_token:short.access_token});
  if(typeof long.access_token!=='string'||!long.access_token||!(Number.isFinite(long.expires_in)&&long.expires_in>0))throw new ProviderError('invalid_token_response');
  return long;
 }
 // Facebook Page tokens cannot use Instagram upgrade or refresh endpoints.
 async upgrade(){throw new ProviderError('facebook_reauthorization_required');}
 async refresh(){throw new ProviderError('facebook_reauthorization_required');}
 async discover(token){
  const grants=await this.graph('me/permissions',token);
  if(!FACEBOOK_SCOPES.every(p=>grants.data?.some(g=>g.permission===p&&g.status==='granted')))throw new ProviderError('facebook_permissions_missing');
  const pages=new Map(),cursors=new Set();let after;
  for(let page=0;page<5;page++){
   const d=await this.graph('me/accounts',token,{fields:'id,name,access_token,tasks',limit:'100',...(after?{after}:{})});
   if(!Array.isArray(d.data))throw new ProviderError('invalid_page_response');
   for(const p of d.data){if(!numeric(p.id)||typeof p.name!=='string'||!p.tasks?.includes('ANALYZE')||typeof p.access_token!=='string'||!p.access_token)continue;pages.set(p.id,p);if(pages.size>10)throw new ProviderError('account_limit');}
   if(!d.paging?.next){if(!pages.size)throw new ProviderError('facebook_no_analyzable_pages');return [...pages.values()];}
   after=d.paging?.cursors?.after;
   if(typeof after!=='string'||after.length>4096||cursors.has(after))throw new ProviderError('facebook_discovery_incomplete');cursors.add(after);
  }
  throw new ProviderError('facebook_discovery_incomplete');
 }
 async profile(token,id){return this.graph(id,token,{fields:'id,name,followers_count'});}
}
const lifetime=(d,metric)=>{const rows=d?.data?.filter(x=>x.name===metric&&x.period==='lifetime');return rows?.length===1&&rows[0].values?.length===1?count(rows[0].values[0].value):null;};
// Sum only a complete set of distinct daily buckets. Never sum unique viewers/reach.
export function dailyTotal(d,metric,start,end){
 const rows=d?.data?.filter(x=>x.name===metric&&x.period==='day');if(rows?.length!==1)return null;
 const raw=rows[0].values;if(!Array.isArray(raw)||raw.some(v=>!Number.isFinite(Date.parse(v.end_time))))return null;
 // Meta rounds since/until to provider days and may include the adjacent ending bucket.
 // A one-day query pad supplies the first requested end_time; discard only outside buckets.
 const values=raw.filter(v=>{const t=Date.parse(v.end_time)/1000;return t>=start&&t<end;});
 if(values.length!==(end-start)/86400)return null;
 const dates=new Set();let total=0;
 for(const row of values){const t=Date.parse(row.end_time)/1000,value=count(row.value),date=Math.floor(t/86400);if(!Number.isFinite(t)||t<start||t>=end||value===null||dates.has(date))return null;dates.add(date);total+=value;}
 return total;
}
function safeUrl(value){try{const u=new URL(value);return u.protocol==='https:'&&(u.hostname==='facebook.com'||u.hostname.endsWith('.facebook.com'))&&!u.username&&!u.password?u.href:null;}catch{return null;}}
export async function collectFacebook(base,c,days,now){
 const api=Object.assign(new Facebook(base.env,base.fetch),{budget:60,signal:AbortSignal.timeout(20000)});
 const until=Math.floor(now/86400000)*86400,since=until-days*86400;
 const result={provider:'facebook',bucketQueryVersion:2,metrics:{followers:null,reach:null,views:null,interactions:null,accountsEngaged:null,engagementRate:null},content:[],series:[],errors:[],truncated:false,range:{since,until,days,timezone:'UTC',metricsScope:'provider_daily_buckets',note:'Facebook: blocos diários da Meta selecionados por end_time; não são eventos delimitados exatamente em UTC.'},lastSynced:new Date(now).toISOString()};
 const error=(metric,code,mediaId)=>result.errors.push({accountId:c.id,metric,code,...(mediaId?{mediaId}:{})});
 const attempt=async(metric,fn,mediaId)=>{try{return await fn();}catch(e){error(metric,e instanceof ProviderError?e.code:'provider_error',mediaId);return null;}};
 if(c.provider!=='facebook'||!numeric(c.providerId)||c.id!==facebookId(c.providerId)){error('account','invalid_provider_account');return {...result,partial:true};}
 if(c.status==='expired'||c.expiresAt<=now){error('token','token_expired');return {...result,partial:true};}
 if(!api.env.FACEBOOK_APP_ID||!api.env.FACEBOOK_APP_SECRET){error('configuration','facebook_not_configured');return {...result,partial:true};}
 const p=await attempt('profile',()=>api.profile(c.token,c.providerId));
 if(p&&p.id===c.providerId){c.profile={id:p.id,name:typeof p.name==='string'?p.name:c.profile.name,followers_count:count(p.followers_count)};result.metrics.followers=count(p.followers_count);}
 else if(p)error('profile','invalid_profile');
 for(const [metric,key]of [['page_media_view','views'],['page_post_engagements','interactions']]){
  const totals=[];
  for(let start=since;start<until;start+=29*86400){const end=Math.min(until,start+29*86400);const d=await attempt(metric,()=>api.graph(c.providerId+'/insights',c.token,{metric,period:'day',since:String(start-86400),until:String(end-1)}));const total=dailyTotal(d,metric,start,end);if(d&&total===null)error(metric,'incomplete_daily_series');totals.push(total);}
  result.metrics[key]=totals.every(x=>x!==null)?totals.reduce((a,b)=>a+b,0):null;
 }
 // No deprecated impressions/reach fallback, no relabelling media viewers as reach.
 error('reach','facebook_reach_unsupported');
 let after;const seen=new Set(),cursors=new Set();
 for(let page=0;page<5;page++){
  const d=await attempt('posts',()=>api.graph(c.providerId+'/posts',c.token,{fields:'id,message,created_time,permalink_url',since:String(since),until:String(until-1),limit:'25',...(after?{after}:{})}));
  if(!d)break;if(!Array.isArray(d.data)){error('posts','invalid_media_response');break;}
  for(const m of d.data){
   if(result.content.length>=12||api.signal.aborted||api.budget<3){result.truncated=true;break;}
   if(typeof m.id!=='string'||!/^\d{1,30}_\d{1,30}$/.test(m.id)||m.id.split('_')[0]!==c.providerId||seen.has(m.id))continue;seen.add(m.id);
   const t=Date.parse(m.created_time);if(!Number.isFinite(t)||t<since*1000||t>=until*1000)continue;
   const item={id:facebookId(m.id),providerId:m.id,provider:'facebook',accountId:c.id,account:c.id,caption:typeof m.message==='string'?m.message:'',type:'post',date:new Date(t).toISOString(),timestamp:new Date(t).toISOString(),url:safeUrl(m.permalink_url),thumbnail:null,reach:null,views:null,likes:null,comments:null,shares:null,saved:null,interactions:null,engagementRate:null,metricsScope:'lifetime',detailsSupported:false};
   for(const [metric,key]of [['post_media_view','views'],['post_reactions_like_total','likes']]){const d=await attempt(metric,()=>api.graph(m.id+'/insights',c.token,{metric,period:'lifetime'}),item.id);item[key]=lifetime(d,metric);if(d&&item[key]===null)error(metric,'metric_unavailable',item.id);}
   const counts=await attempt('post_counts',()=>api.graph(m.id,c.token,{fields:'id,shares,comments.limit(0).summary(true)'}),item.id);
   if(counts?.id===m.id){item.comments=count(counts.comments?.summary?.total_count);item.shares=count(counts.shares?.count);}
   result.content.push(item);
  }
  if(result.truncated||!d.paging?.next)break;after=d.paging?.cursors?.after;
  if(typeof after!=='string'||after.length>4096||cursors.has(after)||page===4){result.truncated=true;break;}cursors.add(after);
 }
 if(result.truncated)error('posts','content_truncated');
 return {...result,partial:result.errors.length>0};
}

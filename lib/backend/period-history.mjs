// Connection-owned encrypted cache. Keep whole snapshots, never mix metric windows.
export function sameHistory(c,entry,days,now){
 const d=entry?.data,until=Math.floor(now/86400000)*86400;
 return !!d&&!authFailure(d)&&c.status!=='expired'&&c.expiresAt>now&&d.range?.since===until-days*86400&&d.range?.until===until&&d.range?.timezone==='UTC'&&(d.provider||'instagram')===(c.provider||'instagram')&&Date.parse(d.lastSynced)<=now&&now-Date.parse(d.lastSynced)<86400000;
}
const authFailure=d=>(d.errors||[]).some(e=>['token_expired','permission_denied','facebook_permissions_missing','invalid_provider_account'].includes(e.code));
export function preserveHistory(c,entry,next,days,now){
 if(authFailure(next))return {...next,stale:false};
 if(!sameHistory(c,entry,days,now)||!next.errors?.length)return {...next,stale:false};
 const prior=entry.data;
 const lost=(a,b)=>Object.entries(a||{}).some(([k,v])=>Number.isFinite(v)&&!Number.isFinite(b?.[k]));
 const metricsLost=lost(prior.metrics,next.metrics);
 const seriesLost=prior.series?.length&&!next.series?.length&&next.errors.some(e=>e.metric==='reach_series');
 const postsFailed=next.errors.some(e=>['media','posts'].includes(e.metric));
 const contentLost=(prior.content||[]).some(p=>{const n=next.content?.find(x=>x.id===p.id);return n?lost(p,n):postsFailed;});
 if(!metricsLost&&!seriesLost&&!contentLost)return {...next,stale:false};
 // Preserve the original query time even over repeated failures/restarts.
 return {...prior,...(next.bucketQueryVersion?{bucketQueryVersion:next.bucketQueryVersion}:{}),stale:true,partial:true,refreshAttemptedAt:next.lastSynced,errors:[...new Map([...next.errors,...(prior.previousErrors||prior.errors)].map(e=>[JSON.stringify(e),e])).values()],previousErrors:prior.previousErrors||prior.errors};
}

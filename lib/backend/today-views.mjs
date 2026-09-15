import {Instagram,ProviderError,number} from './instagram.mjs';
import {Facebook} from './facebook.mjs';
export const todayRange=now=>({since:Math.floor(now/86400000)*86400,until:Math.floor(now/1000),timezone:'UTC',days:0,incomplete:true});
export async function todayViews(api,c,now){
 const range=todayRange(now),result={accountId:c.id,views:null,range,checkedAt:new Date(now).toISOString(),error:null};
 const facebook=c.provider==='facebook';if(facebook)result.provider='facebook';
 if(c.expiresAt<=now)return {...result,error:'token_expired'};
 const client=Object.assign(new (facebook?Facebook:Instagram)(api.env,api.fetch),{budget:1,signal:AbortSignal.timeout(10000)});
 try{
  const d=await client.graph((facebook?c.providerId:c.id)+'/insights',c.token,{metric:facebook?'page_media_view':'views',period:'day',...(!facebook?{metric_type:'total_value'}:{}),since:String(range.since),until:String(range.until)});
  const rows=d.data?.filter(x=>x.name===(facebook?'page_media_view':'views')&&x.period==='day');
  const value=rows?.length===1?number(facebook?rows[0].values?.[0]?.value:rows[0].total_value?.value):null;
  // Real probes show a bucket at 07:00 UTC, not midnight-to-now.
  // total_value has no bucket boundaries; never relabel it as today's UTC events.
  // See TODAY_VIEWS.md. Even a numeric zero is not a verified zero for today.
  result.error=value!==null&&value>=0?'current_day_window_unverified':'metric_unavailable';
 }catch(e){result.error=e instanceof ProviderError?e.code:'provider_error';}
 return result;
}
export function todayPayload(results,accounts,now){
 const known=results.filter(r=>r.views!==null),complete=results.length>0&&known.length===results.length;
 return {mode:'live',connected:accounts.length>0,source:results.some(r=>r.provider==='facebook')?'Instagram + Facebook Pages APIs':'Instagram API',metricsScope:'account_current_day',accounts,accountViews:results,metrics:{views:complete?known.reduce((n,r)=>n+r.views,0):null},knownSubtotal:known.length?known.reduce((n,r)=>n+r.views,0):null,availableAccounts:known.length,selectedAccounts:results.length,partial:!complete,providerDelayHours:48,range:{...todayRange(now),until:results.length?Math.min(...results.map(r=>r.range?.until??Math.floor(now/1000))):Math.floor(now/1000)},requestedRange:todayRange(now),lastSynced:results.map(r=>r.checkedAt).sort()[0]||null,content:[],series:[],errors:results.filter(r=>r.error).map(r=>({accountId:r.accountId,metric:'views',code:r.error})),message:'Hoje em UTC, desde 00:00. Dia incompleto; a Meta pode atrasar os dados em até 48 horas. Views não são pessoas únicas. Não inclui acumulados de publicações.'};
}

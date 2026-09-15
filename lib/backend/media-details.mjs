import {Instagram,ProviderError,number} from './instagram.mjs';
export const MEDIA_METRICS={ig_reels_avg_watch_time:{unit:'milliseconds'},ig_reels_video_view_total_time:{unit:'milliseconds',inDevelopment:true},reels_skip_rate:{unit:'percent',estimated:true,inDevelopment:true}};
// Ownership must be established by the route before this bounded read-only call.
export async function mediaDetails(api,connection,media,now){
 const scoped=Object.assign(new Instagram(api.env,api.fetch),{budget:3,signal:AbortSignal.timeout(12000)}),errors=[],metrics={};
 await Promise.all(Object.entries(MEDIA_METRICS).map(async([metric,meta])=>{
  metrics[metric]={...meta,value:null};
  if(media.type!=='reel'){metrics[metric].reason='not_applicable';return;}
  try{
   if(connection.expiresAt<=now)throw new ProviderError('token_expired');
   const d=await scoped.graph(media.id+'/insights',connection.token,{metric}),row=d.data?.find(x=>x.name===metric);
   metrics[metric].value=number(row?.total_value?.value??row?.values?.[0]?.value);
   if(metrics[metric].value===null)throw new ProviderError('metric_unavailable');
  }catch(e){const code=e instanceof ProviderError?e.code:'provider_error';metrics[metric].reason=code;errors.push({metric,code});}
 }));
 return {source:'Instagram API',accountId:connection.id,media:{...media},metrics,metricsScope:'lifetime',fetchedAt:new Date(now).toISOString(),partial:errors.length>0,errors,retentionCurve:{available:false,reason:'not_exposed_by_media_insights'},viewCountries:{available:false,reason:'not_exposed_by_media_insights'}};
}

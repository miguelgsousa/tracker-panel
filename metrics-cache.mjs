// Memory-only: never persist private analytics in localStorage.
export function createPeriodCache(now=Date.now){
 const values=new Map(),flights=new Map();let generation=0,ownership=null;
 const day=()=>Math.floor(now()/86400000);
 const unauthorized=data=>(data.accounts||[]).some(a=>a.status==='expired'||a.expiresAt&&Date.parse(a.expiresAt)<=now())||(data.errors||[]).some(e=>['token_expired','permission_denied','facebook_permissions_missing'].includes(e.code));
 const clear=()=>{generation++;values.clear();flights.clear();ownership=null;};
 const peek=(key,{stale=false}={})=>{const v=values.get(key);return v&&v.day===day()&&(stale||now()-v.at<300000)&&!unauthorized(v.data)?v.data:null;};
 return {peek,clear,fail(key,error){const previous=peek(key,{stale:true});if(!previous)return null;const data={...previous,stale:true,partial:true,refreshAttemptedAt:new Date(now()).toISOString(),refreshError:error};values.get(key).data=data;return data;},load(key,loader,{force=false}={}){
  const cached=peek(key);if(!force&&cached)return Promise.resolve(cached);
  if(flights.has(key))return flights.get(key);
  const g=generation,d=day();const p=loader().then(data=>{
   if(g===generation&&d===day()){
    if(unauthorized(data)){clear();return data;}
    const signatures=new Map((data.accounts||[]).map(a=>[a.id,JSON.stringify([a.provider,a.connectedAt,a.expiresAt])]));
    if(ownership&&[...signatures].some(([id,sig])=>ownership.has(id)&&ownership.get(id)!==sig)){clear();ownership=signatures;return data;}
    ownership ||= new Map();for(const [id,sig] of signatures)ownership.set(id,sig);
    if(values.size>=80)values.delete(values.keys().next().value);values.set(key,{data,day:d,at:now()});
   }return data;
  }).finally(()=>{if(flights.get(key)===p)flights.delete(key)});
  flights.set(key,p);return p;
 }};
}

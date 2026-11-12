export type Row = {id:number;record_number:number;batch_id:string;filename:string;revision:number;sha256:string;normalized:Record<string,string>;raw:Record<string,string>}
export type Evidence = {internal:Row[];broker:Row[];differences:{field:string;internal:string;broker:string;delta:string|null;tolerance:string|null;rule:string}[];rules:Record<string,unknown>;duplicate_detail?:Record<string,string>;notional_delta?:string;notional_currency?:string}
export type Batch = {id:string;side:string;revision:number;status:string;filename:string;sha256:string;byte_size:number;parsed:number;accepted:number;rejected:number;error:string|null;checks:Record<string,boolean|null>;manifest:Record<string,unknown>;manifest_origin:'PRODUCER'|'OPERATOR'}
export type Work = {id:string;kind:string;ordinal:number;state:string;attempts:number;token:number;pause:boolean;error:string|null;metrics:Record<string,number>;heartbeat_at:string|null}
export type Run = {id:string;scope_id:string;sequence:number;status:string;outcome:string|null;metrics:Record<string,number>;rule_version:string;code_version:string;hash_version:string;bucket_count:number;internal_batch_id:string;broker_batch_id:string;created_at:string;completed_at:string|null;report_status:string;error:string|null;partitions?:Work[];inputs?:Batch[];work?:Work[]}
export type Case = {id:string;reconciliation_ref:string;family:string;workflow:string;detection:string;assignee:string|null;version:number;evidence:Evidence;first_run_id:string;last_run_id:string;created_at:string;updated_at:string;events?:{id:string;actor:string;kind:string;reason:string;created_at:string}[];observations?:{run_id:string;sequence:number;detection:string;evidence:Evidence;created_at:string}[]}
export type Scope = {id:string;broker_name:string;broker_id:string;business_date:string;dataset_id:string;published_run_id:string|null;metrics:Record<string,number>|null;published_sequence:number|null;account_ids:string[]}
export const scopeLabel=(scope:Scope)=>`${scope.broker_name} · ${scope.business_date} · ${scope.dataset_id}`
export class ApiError extends Error { constructor(public status:number,public detail:any){super(typeof detail==='string'?detail:Array.isArray(detail)?detail.map((e:any)=>`${e.loc?.slice(1).join('.')||'Request'}: ${e.msg}`).join('; '):detail?.message||JSON.stringify(detail))} }
export async function api<T=any>(url:string,init?:RequestInit):Promise<T>{
 const res=await fetch('/api'+url,init)
 if(!res.ok){const body=await res.json().catch(()=>({detail:res.statusText}));throw new ApiError(res.status,body.detail)}
 return res.json()
}
export function post<T=any>(url:string,body:unknown){return api<T>(url,{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':crypto.randomUUID()},body:JSON.stringify(body)})}

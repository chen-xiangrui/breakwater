import {useRef,useState,type FormEvent} from 'react'
import {LoaderCircle,Plus,TriangleAlert} from 'lucide-react'
import {api,type Scope} from './api'

const localDay=()=>{
  const now=new Date()
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`
}

export default function ScopeForm({defaults,onCreated,onCancel,onBusyChange}:{
  defaults?:Scope;onCreated:(scope:Scope)=>void;onCancel:()=>void;onBusyChange?:(busy:boolean)=>void
}){
  const [values,setValues]=useState(()=>{
    const day=localDay()
    return {broker_id:defaults?.broker_id||'',broker_name:defaults?.broker_name||'',business_date:day,
      dataset_id:defaults?.dataset_id||'',accounts:defaults?.account_ids.join(', ')||'',
      internal_deadline:day+'T22:00',broker_deadline:day+'T22:30'}
  })
  const [busy,setBusy]=useState(false),[error,setError]=useState('')
  const key=useRef(crypto.randomUUID())
  function update(field:keyof typeof values,value:string){
    key.current=crypto.randomUUID();setError('')
    setValues(previous=>({...previous,[field]:value,...(field==='business_date'?{
      internal_deadline:value+'T'+previous.internal_deadline.slice(11),
      broker_deadline:value+'T'+previous.broker_deadline.slice(11)
    }:{})}))
  }
  async function submit(event:FormEvent){
    event.preventDefault()
    if(busy)return
    const accounts=values.accounts.split(/[,\n]/).map(x=>x.trim()).filter(Boolean)
    if(!accounts.length){setError('Enter at least one account ID.');return}
    if(new Set(accounts).size!==accounts.length){setError('Each account ID should appear only once.');return}
    setBusy(true);onBusyChange?.(true);setError('')
    try{
      const created=await api<Scope>('/scopes',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':key.current},body:JSON.stringify({
        broker_id:values.broker_id,broker_name:values.broker_name,business_date:values.business_date,dataset_id:values.dataset_id,
        account_ids:accounts,internal_deadline:new Date(values.internal_deadline).toISOString(),broker_deadline:new Date(values.broker_deadline).toISOString()
      })})
      onCreated(created)
    }catch(e){setError((e as Error).message)}
    finally{setBusy(false);onBusyChange?.(false)}
  }
  return <form className="scope-form" onSubmit={submit}>
    <fieldset disabled={busy}>
      <div className="scope-fields">
        <label>Broker ID<input required maxLength={80} value={values.broker_id} onChange={e=>update('broker_id',e.target.value)} placeholder="BRK01"/></label>
        <label>Broker name<input required maxLength={160} value={values.broker_name} onChange={e=>update('broker_name',e.target.value)} placeholder="Northstar Securities"/></label>
        <label>Business date<input required type="date" value={values.business_date} onChange={e=>update('business_date',e.target.value)}/></label>
        <label>Dataset ID<input required maxLength={80} value={values.dataset_id} onChange={e=>update('dataset_id',e.target.value)} placeholder="equities_accounts_01"/></label>
        <label className="full-width">Account IDs<textarea aria-label="Account IDs" aria-describedby="scope-account-help" required rows={2} value={values.accounts} onChange={e=>update('accounts',e.target.value)} placeholder="ACC01, ACC02"/><small id="scope-account-help">Separate account IDs with commas or new lines. Both files must cover these accounts.</small></label>
      </div>
      <div className="deadline-heading"><h3>Expected deliveries</h3><p>Set deadlines before the files arrive so missing deliveries can be tracked. Times use your local timezone ({Intl.DateTimeFormat().resolvedOptions().timeZone}).</p></div>
      <div className="scope-fields">
        <label>Internal delivery due<input required type="datetime-local" value={values.internal_deadline} onChange={e=>update('internal_deadline',e.target.value)}/></label>
        <label>Broker delivery due<input required type="datetime-local" value={values.broker_deadline} onChange={e=>update('broker_deadline',e.target.value)}/></label>
      </div>
    </fieldset>
    {error&&<div className="alert error" role="alert"><TriangleAlert size={17}/><span>{error}</span></div>}
    <div className="modal-actions"><button type="button" disabled={busy} className="btn secondary" onClick={onCancel}>Cancel</button><button type="submit" className="btn primary" disabled={busy}>{busy?<LoaderCircle size={15} className="spin"/>:<Plus size={15}/>} {busy?'Creating scope…':'Create scope'}</button></div>
  </form>
}

import {useEffect,useState} from 'react'
import {Plus,TriangleAlert} from 'lucide-react'
import Modal from './Modal'
import ScopeForm from './ScopeForm'
import ManualWorkflow,{type Workflow} from './ManualWorkflow'
import {api,scopeLabel,type Scope} from './api'

export default function ReconciliationModal({scopes,initialScopeId,onScopeCreated,onRegistered,onClose}:{
  scopes:Scope[];initialScopeId:string;onScopeCreated:(scope:Scope)=>void;
  onRegistered:(workflow:Workflow)=>void;onClose:()=>void
}){
  const [available,setAvailable]=useState(scopes),[selected,setSelected]=useState(initialScopeId)
  const [creating,setCreating]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState('')
  const [loading,setLoading]=useState(true)
  const scope=available.find(s=>s.id===selected)
  useEffect(()=>{
    let alive=true
    api<Scope[]>('/scopes').then(result=>{
      if(!alive)return
      setAvailable(result)
      setSelected(previous=>result.some(s=>s.id===previous)?previous:result[0]?.id||'')
    }).catch(e=>{if(alive)setError(e.message)}).finally(()=>{if(alive)setLoading(false)})
    return()=>{alive=false}
  },[])
  function created(result:Scope){
    setAvailable(previous=>[result,...previous.filter(s=>s.id!==result.id)])
    setSelected(result.id);setCreating(false);onScopeCreated(result)
  }
  return <Modal title="New reconciliation" subtitle="Choose the daily scope, then upload the internal and broker snapshots." onClose={onClose} wide busy={busy}>
    <div className="recon-scope-heading"><h3>1. Select a scope</h3><button className="text-link" disabled={busy||creating} onClick={()=>setCreating(true)}><Plus size={14}/>Create missing scope</button></div>
    {error&&<div className="alert error" role="alert"><TriangleAlert size={17}/>{error}</div>}
    {creating?<div className="inline-scope"><h3>Create a scope</h3><p>Define the broker, business date, dataset, and accounts for these files.</p><ScopeForm defaults={scope} onCreated={created} onCancel={()=>setCreating(false)} onBusyChange={setBusy}/></div>:<>
      <label className="recon-scope-label">Reconciliation scope<select disabled={busy||loading} value={selected} onChange={e=>setSelected(e.target.value)}><option value="" disabled>{loading?'Loading scopes…':'Choose a scope'}</option>{available.map(s=><option key={s.id} value={s.id}>{scopeLabel(s)}</option>)}</select></label>
      {!loading&&!available.length&&<p className="scope-help">Create a scope first to define the broker, date, and accounts for your reconciliation.</p>}
      {scope&&<div className="selected-scope"><div><small>Broker ID</small><strong>{scope.broker_id}</strong></div><div><small>Business date</small><strong>{scope.business_date}</strong></div><div><small>Dataset</small><strong>{scope.dataset_id}</strong></div><div><small>Accounts</small><strong>{scope.account_ids.join(', ')}</strong></div></div>}
      <ManualWorkflow key={selected} scope={scope} onRegistered={onRegistered} onBusyChange={setBusy}/>
    </>}
  </Modal>
}

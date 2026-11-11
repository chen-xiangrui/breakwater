import {useEffect,useRef,useState} from 'react'
import {ArrowRight,Check,FileText,LoaderCircle,Play,Upload,X,TriangleAlert} from 'lucide-react'
import {api,type Scope} from './api'

type Workflow = {
  id:string;scope_id:string;status:string;run_id:string|null;run_sequence:number|null;
  internal_batch_id:string;broker_batch_id:string;internal_filename?:string;broker_filename?:string;
  internal_status?:string;broker_status?:string;error:string|null;
}
const activeStates=['WAITING_INPUTS','WAITING_FOR_CAPACITY','QUEUED','RUNNING','FINALIZING']
const statusLabel:Record<string,string>={WAITING_INPUTS:'Validating your files',WAITING_FOR_CAPACITY:'Waiting for processing capacity',QUEUED:'Reconciliation queued',RUNNING:'Reconciling trades',FINALIZING:'Publishing results',SUCCEEDED:'Workflow complete',BLOCKED:'File validation needs attention',FAILED:'Workflow failed',SUPERSEDED:'A newer run has been published'}

export default function ManualWorkflow({scope,onRegistered,onRunReady,onInspect}:{
  scope:Scope|undefined;
  onRegistered:(workflow:Workflow)=>void;
  onRunReady:(id:string)=>void;
  onInspect:(id:string)=>void;
}){
  const [internal,setInternal]=useState<File|null>(null),[broker,setBroker]=useState<File|null>(null)
  const [complete,setComplete]=useState(false),[uploading,setUploading]=useState(false),[error,setError]=useState('')
  const [workflow,setWorkflow]=useState<Workflow|null>(null)
  const key=useRef(crypto.randomUUID()),follow=useRef<string|null>(null)
  const onRunReadyRef=useRef(onRunReady)
  onRunReadyRef.current=onRunReady
  const active=!!workflow&&activeStates.includes(workflow.status)

  useEffect(()=>{
    setWorkflow(null);setInternal(null);setBroker(null);setComplete(false);setError('');follow.current=null
    key.current=crypto.randomUUID()
    if(!scope)return
    let alive=true,pending=false
    async function refresh(){
      if(pending)return
      pending=true
      try{
        const current=await api<Workflow|null>(`/workflows?scope_id=${encodeURIComponent(scope!.id)}`)
        if(alive){
          setWorkflow(current)
          if(current?.run_id&&follow.current===current.id){follow.current=null;onRunReadyRef.current(current.run_id)}
        }
      }catch{/* The workspace connection indicator handles availability. */}
      finally{pending=false}
    }
    refresh();const interval=setInterval(refresh,1500)
    return()=>{alive=false;clearInterval(interval)}
  },[scope?.id])

  function choose(side:'internal'|'broker',file:File|null){
    if(file&&(!file.name.toLowerCase().endsWith('.csv')||file.size>50*1024*1024)){
      setError('Choose a CSV file up to 50 MiB.');return
    }
    setError('');key.current=crypto.randomUUID()
    if(side==='internal')setInternal(file);else setBroker(file)
  }

  async function submit(){
    if(!scope||!internal||!broker||!complete||uploading||active)return
    setUploading(true);setError('')
    const form=new FormData()
    form.set('scope_id',scope.id);form.set('complete','true')
    form.set('internal_file',internal);form.set('broker_file',broker)
    try{
      const created=await api<Workflow>('/workflows',{method:'POST',headers:{'Idempotency-Key':key.current},body:form})
      follow.current=created.id;setWorkflow(created);key.current=crypto.randomUUID();onRegistered(created)
    }catch(e){setError((e as Error).message)}
    finally{setUploading(false)}
  }

  function fileInput(side:'internal'|'broker',file:File|null){return <div className={'manual-file '+(file?'has-file':'')}>
    <div className="manual-file-heading"><h3>{side==='internal'?'Internal CSV':'Broker CSV'}</h3>{file&&<button className="icon-btn" aria-label={`Remove ${side} CSV`} disabled={uploading} onClick={()=>choose(side,null)}><X size={15}/></button>}</div>
    <label className="manual-file-picker"><span className="manual-file-icon">{file?<FileText size={23}/>:<Upload size={23}/>}</span><strong>{file?file.name:`Choose ${side} CSV`}</strong><span>{file?`${(file.size/1024).toFixed(1)} KB · Click to replace`:'Click to browse'}</span><input type="file" aria-label={`${side==='internal'?'Internal':'Broker'} trades CSV`} accept=".csv,text/csv" disabled={uploading} onChange={e=>{choose(side,e.target.files?.[0]||null);e.target.value=''}}/></label>
    <details className="csv-format"><summary>Expected CSV columns</summary><code>{side==='internal'?'trade_id,account,instrument,side,quantity,execution_price,currency,executed_at':'execution_ref,client_account,symbol,buy_sell,filled_qty,fill_price,ccy,execution_time'}</code></details>
  </div>}

  return <section className="panel manual-workflow" aria-label="Upload CSVs and run workflow" id="manual-workflow">
    <div className="panel-heading"><h2>Upload files</h2><span className="file-hint">CSV · up to 50 MiB each</span></div>
    <div className="manual-files">{fileInput('internal',internal)}{fileInput('broker',broker)}</div>
    <div className="manual-submit"><div><label className="complete-declaration"><input type="checkbox" checked={complete} disabled={uploading} onChange={e=>{setComplete(e.target.checked);key.current=crypto.randomUUID()}}/>Both files are complete snapshots for this broker, date, and accounts.</label><p>No manifest required. Completeness is declared by you.</p></div><button className="btn primary" disabled={!scope||!internal||!broker||!complete||uploading||active} onClick={submit}>{uploading?<LoaderCircle className="spin" size={16}/>:<Play size={15} fill="currentColor"/>}{uploading?'Uploading files…':active?'Workflow in progress':'Run workflow'}{!uploading&&!active&&<ArrowRight size={15}/>}</button></div>
    {error&&<div className="alert error" role="alert"><TriangleAlert size={17}/>{error}</div>}
    {workflow&&!workflow.run_id&&<div className={'manual-status '+(workflow.status==='BLOCKED'||workflow.status==='FAILED'?'blocked':'')} role="status" aria-label="Manual workflow status"><span className="manual-status-icon">{active?<LoaderCircle size={19} className="spin"/>:workflow.status==='SUCCEEDED'?<Check size={19}/>:<TriangleAlert size={19}/>}</span><div><strong>{statusLabel[workflow.status]||workflow.status}</strong><p>{workflow.error||`${workflow.internal_filename||internal?.name||'Internal CSV'} + ${workflow.broker_filename||broker?.name||'Broker CSV'}`}</p>{workflow.status==='BLOCKED'&&<div className="manual-validation-links"><button className="text-link" onClick={()=>onInspect(workflow.internal_batch_id)}>Internal validation</button><button className="text-link" onClick={()=>onInspect(workflow.broker_batch_id)}>Broker validation</button></div>}</div>{workflow.run_id&&<button className="text-link push" onClick={()=>onRunReady(workflow.run_id!)}>View run #{workflow.run_sequence} <ArrowRight size={14}/></button>}</div>}
  </section>
}

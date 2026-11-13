import {useEffect,useState,type ReactNode} from 'react'
import {ArrowDownToLine,ArrowRight,Check,CheckCheck,ChevronDown,ChevronLeft,ChevronRight,Clock3,FileCheck2,FileText,GitCompareArrows,History,Layers3,LoaderCircle,MoreHorizontal,Play,Plus,RefreshCw,Search,ShieldCheck,SlidersHorizontal,Upload,Users,X,TriangleAlert} from 'lucide-react'
import {WorkflowStatus,type Workflow} from './ManualWorkflow'
import Modal from './Modal'
import ScopeForm from './ScopeForm'
import ReconciliationModal from './ReconciliationModal'
import {api,post,scopeLabel,type Batch,type Case,type Run,type Scope,type Work,type Evidence} from './api'

const labels:Record<string,string>={FIELD_MISMATCH:'Field mismatch',MISSING_BROKER:'Missing at broker',MISSING_INTERNAL:'Missing internally',DUPLICATE_INTERNAL:'Internal duplicate',DUPLICATE_BROKER:'Broker duplicate',NOT_PRESENT_IN_INPUTS:'Needs review',UNASSESSABLE:'Needs review'}
const nice=(s:string)=>labels[s]||s.toLowerCase().replaceAll('_',' ').replace(/^./,x=>x.toUpperCase())
const fmt=(n:number|undefined)=>new Intl.NumberFormat('en-US').format(n||0)
const time=(s:string)=>new Date(s).toLocaleString('en-GB',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'})
const short=(s:string)=>s.slice(0,8)
const fields=['account','instrument','side','quantity','execution_price','currency']
const fieldName=(s:string)=>s==='execution_price'?'Execution price':nice(s)
const running=(r:Run)=>['QUEUED','RUNNING','FINALIZING'].includes(r.status)

function Badge({value}:{value:string}){return <span className={'badge '+value.toLowerCase()}><i/>{nice(value)}</span>}
function Empty({icon=<Layers3 size={25}/>,title,children}:{icon?:ReactNode;title:string;children?:ReactNode}){return <div className="empty"><div className="empty-icon">{icon}</div><h3>{title}</h3>{children}</div>}
export default function App(){
 const initial=new URLSearchParams(location.search)
 const [view,setView]=useState<'reconciliation'|'history'>(location.hash==='#history'?'history':'reconciliation')
 const [scopes,setScopes]=useState<Scope[]>([]),[sid,setSid]=useState(initial.get('scope')||'')
 const [reconOpen,setReconOpen]=useState(false),[scopeOpen,setScopeOpen]=useState(false),[creatingScope,setCreatingScope]=useState(false)
 const [latestWorkflow,setLatestWorkflow]=useState<Workflow|null>(null),[followWorkflow,setFollowWorkflow]=useState<string|null>(null)
 const [deliveries,setDeliveries]=useState<any[]>([]),[runs,setRuns]=useState<Run[]>([])
 const [runId,setRunId]=useState<string|null>(null),[run,setRun]=useState<Run|null>(null)
 const [queue,setQueue]=useState<{items:Case[];total:number;summary:any[]}>({items:[],total:0,summary:[]})
 const [q,setQ]=useState(initial.get('q')||''),[family,setFamily]=useState(initial.get('family')||'')
 const [detection,setDetection]=useState(initial.get('detection')||''),[workflow,setWorkflow]=useState(initial.get('workflow')||''),[owner,setOwner]=useState(initial.get('assignee')||'')
 const [offset,setOffset]=useState(0),[filtersOpen,setFiltersOpen]=useState(true)
 const [actor,setActor]=useState('Alice'),[caseId,setCaseId]=useState<string|null>(initial.get('case'))
 const [caseDetail,setCaseDetail]=useState<Case|null>(null),[note,setNote]=useState(''),[caseError,setCaseError]=useState(''),[observation,setObservation]=useState<number|null>(null)
 const [uploadSide,setUploadSide]=useState<string|null>(null),[confirmRun,setConfirmRun]=useState(false),[demoOpen,setDemoOpen]=useState(false)
 const [busy,setBusy]=useState(false),[notice,setNotice]=useState(''),[error,setError]=useState(''),[connected,setConnected]=useState(false)
 const [batchDetail,setBatchDetail]=useState<any>(null),[rejections,setRejections]=useState<any[]>([])
 const [selectedInputs,setSelectedInputs]=useState<Record<string,string>>({}),[demoMode,setDemoMode]=useState(false),[pauseNext,setPauseNext]=useState(false),[failNext,setFailNext]=useState(false)
 const [moreOpen,setMoreOpen]=useState(false),[operations,setOperations]=useState<any>(null)
 const [inspectedRun,setInspectedRun]=useState<Run|null>(null),[results,setResults]=useState<any>(null),[resultOutcome,setResultOutcome]=useState(''),[resultOffset,setResultOffset]=useState(0)
 const [group,setGroup]=useState<any>(null),[diff,setDiff]=useState<any>(null)
 const scope=scopes.find(x=>x.id===sid)
 const selected=(side:string):Batch|undefined=>{const bs=deliveries.find(d=>d.side===side)?.batches||[];return bs.find((b:Batch)=>b.id===selectedInputs[side])||bs[0]}
 const internal=selected('INTERNAL'),broker=selected('BROKER')
 const ready=internal?.status==='READY'&&broker?.status==='READY'
 const published=runs.find(r=>r.id===scope?.published_run_id)
 const currentRun=runId||runs.find(running)?.id||scope?.published_run_id||runs[0]?.id
 const displayRun=run?.status==='SUCCEEDED'?run:published
 const metrics=displayRun?.metrics||{}
 const detailRun=inspectedRun?.id===run?.id?run:inspectedRun

 useEffect(()=>{const changed=()=>setView(location.hash==='#history'?'history':'reconciliation');window.addEventListener('hashchange',changed);return()=>window.removeEventListener('hashchange',changed)},[])
 useEffect(()=>{api('/health').then(x=>setDemoMode(x.demo_mode)).catch(()=>{})},[])
 useEffect(()=>{
  const p=new URLSearchParams()
  for(const [key,value] of Object.entries({scope:sid,q,family,detection,workflow,assignee:owner,case:caseId||''})){if(value)p.set(key,value)}
  if(detection==='')p.set('detection','')
  history.replaceState(null,'',`${location.pathname}${p.size?'?'+p:''}#${view}`)
 },[sid,q,family,detection,workflow,owner,caseId,view])
 useEffect(()=>{setOffset(0)},[q,family,detection,workflow,owner,sid])
 useEffect(()=>{
  let alive=true,pending=false
  async function refresh(){
   if(pending)return
   pending=true
   try{
    const ss=await api<Scope[]>('/scopes')
    if(!alive)return
    setScopes(ss)
    if(!ss.some(s=>s.id===sid)){setSid(ss[0]?.id||'');setConnected(true);return}
    const params=new URLSearchParams({scope_id:sid,q,family,detection,workflow,assignee:owner,offset:String(offset),limit:'8'})
    const [ds,rr,cc,ww]=await Promise.all([api(`/scopes/${sid}/deliveries`),api<Run[]>(`/runs?scope_id=${sid}`),api(`/cases?${params}`),api<Workflow|null>(`/workflows?scope_id=${sid}`)])
    if(alive){setDeliveries(ds.deliveries);setRuns(rr);setQueue(cc);setLatestWorkflow(ww);setConnected(true)}
   }catch(e){if(alive){setConnected(false);setError((e as Error).message)}}finally{pending=false}
  }
  refresh();const id=setInterval(refresh,1500);return()=>{alive=false;clearInterval(id)}
 },[sid,q,family,detection,workflow,owner,offset])
 useEffect(()=>{
  if(latestWorkflow?.id===followWorkflow&&latestWorkflow.run_id){setRunId(latestWorkflow.run_id);setRun(null);setFollowWorkflow(null)}
 },[latestWorkflow,followWorkflow])
 useEffect(()=>{
  if(!currentRun){setRun(null);return}
  let alive=true
  const refresh=()=>api<Run>(`/runs/${currentRun}`).then(r=>{if(alive)setRun(r)}).catch(()=>{})
  refresh();const id=setInterval(refresh,1500);return()=>{alive=false;clearInterval(id)}
 },[currentRun])
 useEffect(()=>{
  if(!caseId){setCaseDetail(null);return}
  let alive=true;setCaseDetail(null);setCaseError('');setNote('');setObservation(null)
  api<Case>(`/cases/${caseId}`).then(c=>{if(alive)setCaseDetail(c)}).catch(e=>{if(alive)setCaseError(e.message)})
  return()=>{alive=false}
 },[caseId])
 useEffect(()=>{
  if(!inspectedRun)return
  let alive=true
  api(`/runs/${inspectedRun.id}/results?outcome=${resultOutcome}&offset=${resultOffset}`).then(x=>{if(alive)setResults(x)}).catch(e=>setError(e.message))
  return()=>{alive=false}
 },[inspectedRun?.id,run?.status,resultOutcome,resultOffset])
 useEffect(()=>{
  if(!batchDetail||['READY','BLOCKED','FAILED'].includes(batchDetail.status))return
  let alive=true
  const id=setInterval(()=>Promise.all([api(`/batches/${batchDetail.id}`),api(`/batches/${batchDetail.id}/rejections`)]).then(([b,r])=>{if(alive){setBatchDetail(b);setRejections(r.items)}}).catch(()=>{}),1500)
  return()=>{alive=false;clearInterval(id)}
 },[batchDetail?.id,batchDetail?.status])
 useEffect(()=>{if(!notice)return;const id=setTimeout(()=>setNotice(''),5000);return()=>clearTimeout(id)},[notice])
 useEffect(()=>{if(!caseId)return;const fn=(e:KeyboardEvent)=>{if(e.key==='Escape')setCaseId(null)};window.addEventListener('keydown',fn);return()=>window.removeEventListener('keydown',fn)},[caseId])

 async function act(fn:()=>Promise<unknown>,message?:string){setBusy(true);setError('');try{await fn();if(message)setNotice(message)}catch(e){setError((e as Error).message)}finally{setBusy(false)}}
 function selectScope(id:string){
  if(id===sid)return
  setSid(id);setDeliveries([]);setRuns([]);setRun(null);setRunId(null);setSelectedInputs({})
  setLatestWorkflow(null);setFollowWorkflow(null);setQueue({items:[],total:0,summary:[]});setCaseId(null);setInspectedRun(null);setError('')
 }
 function addScope(created:Scope){setScopes(previous=>[created,...previous.filter(s=>s.id!==created.id)])}
 function registered(w:Workflow){
  selectScope(w.scope_id);setSelectedInputs({INTERNAL:w.internal_batch_id,BROKER:w.broker_batch_id})
  setLatestWorkflow(w);setFollowWorkflow(w.id);setRunId(null);setReconOpen(false);setView('reconciliation');setNotice('Files received. Validation and reconciliation will continue in the background.')
 }
 async function inspectBatch(b:Batch){await act(async()=>{const [detail,reject]=await Promise.all([api(`/batches/${b.id}`),api(`/batches/${b.id}/rejections`)]);setBatchDetail(detail);setRejections(reject.items)})}
 async function inspectRun(id:string){await act(async()=>{setResults(null);setDiff(null);setResultOutcome('');setResultOffset(0);setInspectedRun(await api<Run>(`/runs/${id}`))})}
 async function loadDemo(name:string){await act(async()=>{const loaded=await post<{batches:Batch[]}>(`/demo/scenarios/${name}`,{scope_id:sid});setDeliveries(ds=>ds.map(d=>({...d,batches:[...loaded.batches.filter(b=>b.side===d.side),...d.batches]})));setSelectedInputs({});setDemoOpen(false);setMoreOpen(true)},'Files received. Open existing inputs to run them when validation completes.')}
 async function launch(){await act(async()=>{const r=await post<Run>('/runs',{scope_id:sid,internal_batch_id:internal!.id,broker_batch_id:broker!.id,rule_version:'execution-v1',bucket_count:8,pause_bucket:pauseNext?0:null,fail_bucket:failNext?1:null});setRunId(r.id);setRun(r);setConfirmRun(false);setPauseNext(false);setFailNext(false);setMoreOpen(false)},'Reconciliation started.')}
 async function caseAct(action:string){if(!caseDetail)return;setBusy(true);setCaseError('');try{await post(`/cases/${caseDetail.id}/actions`,{expected_version:caseDetail.version,actor,action,reason:note});setCaseDetail(await api(`/cases/${caseDetail.id}`));setNote('');setNotice('Investigation updated.')}catch(e){setCaseError((e as Error).message)}finally{setBusy(false)}}

 function runDetails(r:Run){return <div className="run-details-content">
  <div className="input-pair">{r.inputs?.map(b=><button key={b.id} onClick={()=>inspectBatch(b)}><FileText size={16}/><span><strong>{nice(b.side)} · revision {b.revision}</strong><small>{b.filename}</small></span><ChevronRight size={14}/></button>)}</div>
  <div className="partition-caption"><span>{r.partitions?.filter(p=>p.state==='SUCCEEDED').length||0} / {r.bucket_count} partitions complete</span><small>{r.rule_version}</small></div>
  <div className="partition-grid">{Array.from({length:r.bucket_count},(_,i)=>{const p=r.partitions?.find(p=>p.ordinal===i);return <div key={i} className={'partition '+(p?.state.toLowerCase()||'pending')} title={p?.error||''}><div><span>P{i+1}</span>{p?.state==='SUCCEEDED'?<Check size={13}/>:p?.state==='RUNNING'?<LoaderCircle size={13} className="spin"/>:null}</div><strong>{p?.pause?'Paused':nice(p?.state||'PENDING')}</strong><small>{p?.attempts||0} attempt{p?.attempts===1?'':'s'}</small>{p?.pause&&demoMode&&<button className="text-link" onClick={()=>act(()=>post(`/demo/work/${p.id}`,{action:'release'}))}>Release</button>}{p?.error&&<small className="red">{p.error}</small>}</div>})}</div>
  <dl className="run-meta"><div><dt>Run ID</dt><dd className="mono">{r.id}</dd></div><div><dt>Rules / code</dt><dd>{r.rule_version} / {r.code_version}</dd></div><div><dt>Started</dt><dd>{time(r.created_at)}</dd></div><div><dt>Published</dt><dd>{r.completed_at?time(r.completed_at):'Pending'}</dd></div></dl>
 </div>}

 function historyTable(){return <><div className="table-scroll"><table className="history-table"><thead><tr><th>Run</th><th>Created</th><th>Status</th><th>Matched</th><th>Issues</th><th/></tr></thead><tbody>{runs.map(r=><tr className="clickable" key={r.id} onClick={()=>inspectRun(r.id)}><td>#{r.sequence}</td><td>{time(r.created_at)}</td><td><Badge value={r.status}/></td><td>{fmt(r.metrics.matched)}</td><td>{fmt(r.metrics.findings)}</td><td><button className="icon-btn" aria-label={`View run ${r.sequence}`}><ChevronRight size={16}/></button></td></tr>)}</tbody></table></div>{!runs.length&&<Empty title="No runs yet"><p>Your first reconciliation will appear here.</p></Empty>}</>}

 function deliveryCard(side:string){
  const d=deliveries.find(x=>x.side===side),b=selected(side)
  return <article className="delivery-card" key={side}>
   <div className="delivery-title"><FileText size={18}/><h3>{side==='INTERNAL'?'Internal books':'Broker statement'}</h3><span className="push">{b?<Badge value={b.status}/>:<Badge value={d?.state||'EXPECTED'}/>}</span></div>
   {b?<><div className="delivery-file"><button className="text-link truncate" onClick={()=>inspectBatch(b)}>{b.filename}</button><select aria-label={`${side} source revision`} value={b.id} onChange={e=>setSelectedInputs(x=>({...x,[side]:e.target.value}))}>{d.batches.map((x:Batch)=><option key={x.id} value={x.id}>Revision {x.revision}</option>)}</select></div>
   <div className="delivery-counts"><span><strong>{fmt(b.accepted)}</strong> accepted</span><span className={b.rejected?'red':''}><strong>{fmt(b.rejected)}</strong> rejected</span><span>{(b.byte_size/1024).toFixed(1)} KB</span></div>
   <p className={'delivery-check '+(b.status==='BLOCKED'?'red':'')}>{b.status==='READY'?<ShieldCheck size={14}/>:<Clock3 size={14}/>}<span>{b.status==='READY'?(b.manifest_origin==='OPERATOR'?'CSV checks passed · completeness declared by uploader':'Integrity & completeness controls passed'):b.error||'Validating delivery'}</span></p></>:<p className="delivery-awaiting">File not received{d?` · expected ${time(d.deadline)}`:''}</p>}
   <div className="delivery-bottom"><span className="mono">{b?`SHA ${b.sha256.slice(0,12)}`:'A complete file is required'}</span><button className="text-link" onClick={()=>setReconOpen(true)}>{b?'Upload new pair':'Upload files'} <ArrowRight size={12}/></button></div>
  </article>
 }

 return <div className="sidebar-shell">
  <aside className="sidebar" aria-label="Workspace sidebar">
   <a href="#reconciliation" className="brand" aria-label="Breakwater home" onClick={()=>setView('reconciliation')}><span className="brand-mark"><i/><i/></span><span className="brand-word">breakwater<span className="brand-dot">.</span></span></a>
   <nav aria-label="Main navigation">
    <button aria-current={view==='reconciliation'?'page':undefined} className={view==='reconciliation'?'active':''} onClick={()=>setView('reconciliation')}><GitCompareArrows size={18}/><span>Reconciliation</span></button>
    <button aria-current={view==='history'?'page':undefined} className={view==='history'?'active':''} onClick={()=>setView('history')}><History size={18}/><span>Run history</span></button>
   </nav>
   <div className="sidebar-footer"><span className="connection"><span className={connected?'live-dot':'offline-dot'}/>{connected?'Connected':'Reconnecting'}</span><button className="btn quiet" aria-label="More options" onClick={()=>setMoreOpen(true)}><SlidersHorizontal size={15}/><span>Advanced options</span></button></div>
  </aside>
  <div className="content-shell"><main className="workspace-main">
   <div className="workspace-heading"><div><h1>{view==='history'?'Run history':'Reconcile your trades'}</h1><p>{view==='history'?'Review previous runs, comparisons, and reports.':'Prepare the day’s scope. Reconcile files. Investigate the differences.'}</p></div></div>
   {error&&<div className="alert error" role="alert"><TriangleAlert size={17}/><span>{error}</span><button className="icon-btn push" aria-label="Dismiss error" onClick={()=>setError('')}><X size={16}/></button></div>}
   {notice&&<div className="toast" role="status"><Check size={16}/>{notice}</div>}
   <section className="scope-actions" aria-label="Daily reconciliation setup"><div><h2>{scope?'Ready for the next reconciliation?':'Set up your first reconciliation'}</h2><p>Create a scope for the day, then upload a pair of files to start a run.</p></div><div className="scope-action-buttons"><button className="btn secondary" onClick={()=>setScopeOpen(true)}><Plus size={15}/>Create scope</button><button className="btn primary" onClick={()=>setReconOpen(true)}><Play size={15}/>New reconciliation</button></div></section>
   <div className="scope-context" role="group" aria-label="Scope selection">
    <label htmlFor="workspace-scope">Scope</label>
    {scopes.length>0?<select id="workspace-scope" aria-label="Reconciliation scope" value={sid} onChange={e=>selectScope(e.target.value)}>{scopes.map(s=><option key={s.id} value={s.id}>{scopeLabel(s)}</option>)}</select>:<span>{connected?'No scopes yet':'Loading workspace…'}</span>}
    <small>{scope?`${scope.broker_id} · ${scope.account_ids.length} accounts`:'Create a scope to get started'}</small>
   </div>
   <div className="reconciliation-view" hidden={view!=='reconciliation'}>
   <WorkflowStatus workflow={latestWorkflow} onInspect={id=>act(async()=>inspectBatch(await api(`/batches/${id}`)))}/>

   {(run||displayRun)&&<section className="panel results-panel" id="results" aria-label="Reconciliation results">
    <div className="result-heading"><div className="result-run"><span>Run #{run?.sequence||displayRun?.sequence}</span><Badge value={run?.status||displayRun!.status}/>{run&&running(run)&&<small>{run.partitions?.filter(p=>p.state==='SUCCEEDED').length||0} / {run.bucket_count} partitions complete</small>}</div><div className="result-actions">{displayRun?.report_status==='READY'&&<a className="btn secondary small-btn" href={`/api/runs/${displayRun.id}/report`}><ArrowDownToLine size={14}/>Download report</a>}{displayRun&&<button className="icon-btn" aria-label="Inspect run" onClick={()=>inspectRun(displayRun.id)}><ChevronRight size={18}/></button>}</div></div>
    {run?.error&&<div className="alert error"><TriangleAlert size={16}/>{run.error}</div>}
    {displayRun?<><div className="result-totals"><div className="result-stat"><strong>{fmt(metrics.groups)}</strong><span>Total trade groups</span></div><div className="result-stat matched"><strong>{fmt(metrics.matched)}</strong><span>Matched <CheckCheck size={14}/></span></div><div className="result-stat breaks"><strong>{fmt(metrics.findings)}</strong><span>Discrepancies</span></div><div className="result-rate"><strong>{metrics.groups?((metrics.matched||0)/metrics.groups*100).toFixed(1):'0'}%</strong><span>match rate</span></div></div>{run&&run.id!==displayRun.id&&<p className="freshness">Showing published results from run #{displayRun.sequence} while the selected run is {nice(run.status).toLowerCase()}.</p>}</>:<p className="processing-note">Results will appear when validation and reconciliation are complete.</p>}
    {run&&<details className="technical-details"><summary>Processing details <ChevronDown size={14}/></summary>{runDetails(run)}</details>}
   </section>}

   {scope&&<section className="source-deliveries" aria-label="Source deliveries"><div className="source-heading"><h2>Source deliveries</h2><button className="text-link" disabled={!ready||busy} onClick={()=>setConfirmRun(true)}>Compare existing inputs <Play size={12}/></button></div><div className="deliveries-grid">{deliveryCard('INTERNAL')}{deliveryCard('BROKER')}</div></section>}

   <section className="panel cases-panel" aria-label="Discrepancies">
    <div className="queue-heading"><div><h2>Discrepancies <span className="count-tag">{queue.total}</span></h2></div><div className="queue-controls"><select aria-label="Detection filter" value={detection} onChange={e=>setDetection(e.target.value)}><option value="PRESENT">Current</option><option value="CLEARED">Cleared</option><option value="NOT_PRESENT_IN_INPUTS">Needs review</option><option value="">All cases</option></select><button className={'icon-btn '+(filtersOpen?'selected':'')} aria-label="Filter discrepancies" aria-expanded={filtersOpen} onClick={()=>setFiltersOpen(x=>!x)}><SlidersHorizontal size={17}/></button></div></div>
    <div className="queue-search"><Search size={16}/><input aria-label="Search cases" placeholder="Search a trade reference…" value={q} onChange={e=>setQ(e.target.value)}/><span>{scope?.account_ids.length||0} accounts</span></div>
    {filtersOpen&&<div className="filters"><select aria-label="Discrepancy filter" value={family} onChange={e=>setFamily(e.target.value)}><option value="">All issue types</option>{['FIELD_MISMATCH','MISSING_BROKER','MISSING_INTERNAL','DUPLICATE_INTERNAL','DUPLICATE_BROKER'].map(x=><option key={x} value={x}>{nice(x)}</option>)}</select><select aria-label="Workflow filter" value={workflow} onChange={e=>setWorkflow(e.target.value)}><option value="">All statuses</option>{['OPEN','IN_REVIEW','WAIVED','RESOLVED'].map(x=><option key={x} value={x}>{nice(x)}</option>)}</select><select aria-label="Owner filter" value={owner} onChange={e=>setOwner(e.target.value)}><option value="">All owners</option><option>Alice</option><option>Bob</option></select><button className="text-link" onClick={()=>{setQ('');setFamily('');setWorkflow('');setDetection('');setOwner('')}}>Reset filters</button></div>}
    <div className="table-scroll"><table className="case-table"><thead><tr><th>Reference</th><th>Discrepancy</th><th>Instrument / account</th><th>Difference</th><th>Status</th><th>Owner</th><th/></tr></thead><tbody>{queue.items.map(c=>{const row=(c.evidence.internal[0]||c.evidence.broker[0])?.normalized,d=c.evidence.differences[0];return <tr key={c.id} tabIndex={0} onClick={()=>setCaseId(c.id)} onKeyDown={e=>{if(e.key==='Enter')setCaseId(c.id)}}><td><span className="reference">{c.reconciliation_ref}</span><span className="subline mono case-ref-id">BW-{short(c.id).toUpperCase()}</span></td><td><span className={'issue-dot '+c.family.toLowerCase()}/>{nice(c.family)}</td><td><strong>{row?.instrument||'—'}</strong><span className="subline">{row?.account||'—'} · {row?.side||'—'}</span></td><td>{d?<><span>{fieldName(d.field)}</span><span className="subline mono">{d.internal} → {d.broker}</span></>:<span className="muted">{c.evidence.internal.length} internal / {c.evidence.broker.length} broker</span>}</td><td><Badge value={c.workflow}/>{c.detection!=='PRESENT'&&<span className="subline">{nice(c.detection)}</span>}</td><td>{c.assignee?<span className="owner"><span className="avatar small">{c.assignee[0]}</span>{c.assignee}</span>:<span className="muted">Unassigned</span>}</td><td><ChevronRight size={16}/></td></tr>})}</tbody></table></div>
    {!queue.items.length&&<Empty icon={<CheckCheck size={25}/>} title={published?'No discrepancies in this view':'Your results will appear here'}><p>{published?'All clear here. Choose another filter to review previous cases.':'Choose New reconciliation to select a scope and upload your two CSV files.'}</p></Empty>}
    <div className="table-footer"><span>{queue.total?`${offset+1}–${Math.min(offset+8,queue.total)} of ${queue.total} cases`:'0 cases'}</span><div><button className="icon-btn" aria-label="Previous case page" disabled={!offset} onClick={()=>setOffset(x=>Math.max(0,x-8))}><ChevronLeft size={16}/></button><button className="icon-btn" aria-label="Next case page" disabled={offset+8>=queue.total} onClick={()=>setOffset(x=>x+8)}><ChevronRight size={16}/></button></div></div>
   </section>
   </div>
   <section className="panel history-panel" hidden={view!=='history'} aria-label="Run history">{historyTable()}</section>
   <footer className="workspace-footer"><span>Every result keeps its source files and investigation history.</span><button className="text-link" onClick={()=>setMoreOpen(true)}>Advanced options</button></footer>
  </main></div>



  {scopeOpen&&<Modal title="Create scope" subtitle="Prepare a broker, business date, and dataset before the files arrive." onClose={()=>setScopeOpen(false)} busy={creatingScope} wide><ScopeForm defaults={scope} onBusyChange={setCreatingScope} onCancel={()=>setScopeOpen(false)} onCreated={created=>{addScope(created);selectScope(created.id);setScopeOpen(false);setNotice('Scope ready. Choose New reconciliation when your files arrive.')}}/></Modal>}
  {reconOpen&&<ReconciliationModal scopes={scopes} initialScopeId={sid} onScopeCreated={addScope} onRegistered={registered} onClose={()=>setReconOpen(false)}/>}

  {detailRun&&<Modal title={`Run #${detailRun.sequence}`} subtitle="Immutable inputs and comparison results." onClose={()=>setInspectedRun(null)} wide>
   <div className="run-dialog-heading"><Badge value={detailRun.status}/><div className="push result-actions">{runs.some(x=>x.sequence<detailRun.sequence&&x.status==='SUCCEEDED')&&['SUCCEEDED','SUPERSEDED'].includes(detailRun.status)&&<button className="btn secondary small-btn" onClick={()=>act(async()=>{const prev=runs.find(x=>x.sequence<detailRun.sequence&&x.status==='SUCCEEDED')!;setDiff(await api(`/runs/${detailRun.id}/diff?previous_run_id=${prev.id}`))})}>Compare previous</button>}{detailRun.report_status==='READY'&&<a className="btn secondary small-btn" href={`/api/runs/${detailRun.id}/report`}><ArrowDownToLine size={14}/>Download report</a>}</div></div>
   {diff&&<div className="diff-counts">{Object.entries(diff.counts).map(([key,value])=><div key={key}><strong>{String(value)}</strong><span>{nice(key)}</span></div>)}</div>}
   {runDetails(detailRun)}
   <div className="comparison-heading"><h3>Comparison results</h3><select aria-label="Result filter" value={resultOutcome} onChange={e=>{setResultOutcome(e.target.value);setResultOffset(0)}}><option value="">All outcomes</option>{['MATCHED','MISMATCH','MISSING','DUPLICATE'].map(x=><option key={x}>{x}</option>)}</select></div>
   {results?.provisional&&<p className="readiness-note">These results are provisional until the run publishes.</p>}
   <div className="table-scroll"><table><thead><tr><th>Reference</th><th>Outcome</th><th>Internal rows</th><th>Broker rows</th><th>Changed fields</th></tr></thead><tbody>{results?.items.map((g:any)=><tr className="clickable" key={g.id} onClick={()=>setGroup(g)}><td className="mono">{g.reconciliation_ref}</td><td><Badge value={g.outcome}/></td><td>{g.internal_count}</td><td>{g.broker_count}</td><td>{g.evidence.differences.map((d:any)=>fieldName(d.field)).join(', ')||'—'}</td></tr>)}</tbody></table></div><div className="table-footer"><span>{results?.total||0} groups</span><div><button className="icon-btn" aria-label="Previous results" disabled={!resultOffset} onClick={()=>setResultOffset(x=>x-25)}><ChevronLeft size={16}/></button><button className="icon-btn" aria-label="Next results" disabled={resultOffset+25>=(results?.total||0)} onClick={()=>setResultOffset(x=>x+25)}><ChevronRight size={16}/></button></div></div>
  </Modal>}

  {moreOpen&&<Modal title="Advanced options" subtitle="Input revisions and tools for a deeper demo." onClose={()=>setMoreOpen(false)}>
   <details className="advanced-section"><summary>Existing inputs & manifests</summary><div className="advanced-body">{['INTERNAL','BROKER'].map(side=><div className="existing-input" key={side}><label>{nice(side)}<select aria-label={`${side} revision`} value={selected(side)?.id||''} onChange={e=>setSelectedInputs(x=>({...x,[side]:e.target.value}))}><option value="" disabled>No file received</option>{deliveries.find(d=>d.side===side)?.batches.map((b:Batch)=><option value={b.id} key={b.id}>r{b.revision} · {b.filename} · {nice(b.status)}</option>)}</select></label><div>{selected(side)&&<button className="text-link" onClick={()=>inspectBatch(selected(side)!)}>View validation</button>}<button className="text-link" onClick={()=>{setMoreOpen(false);setUploadSide(side)}}>Upload {side.toLowerCase()} with manifest</button></div></div>)}<button className="btn primary" disabled={!ready||busy} onClick={()=>{setMoreOpen(false);setConfirmRun(true)}}><Play size={14}/>Run selected inputs</button></div></details>
   {demoMode&&<details className="advanced-section"><summary>Demo & recovery tools</summary><div className="advanced-body"><p>Load an example, or use the run confirmation to pause a partition or inject a transient failure.</p><button className="btn secondary" onClick={()=>{setMoreOpen(false);setDemoOpen(true)}}>Demo scenarios</button><button className="text-link" onClick={()=>act(async()=>setOperations(await api('/operations')))}>Inspect worker activity</button>{operations&&<div className="attempt-list">{operations.recent_attempts.map((a:any)=><div key={a.id}><span>{nice(a.kind)} · attempt {a.token}</span><Badge value={a.state}/><small>{a.error||'Completed normally'}</small></div>)}</div>}</div></details>}
   <a className="text-link api-doc-link" href="http://localhost:8000/docs" target="_blank" rel="noreferrer">API documentation <ArrowRight size={14}/></a>
  </Modal>}

 {demoOpen&&<Modal title="Load a demo scenario" subtitle="Deterministic synthetic trades. Deliveries use the real ingestion pipeline." onClose={()=>setDemoOpen(false)}><div className="scenario-list">{[['breaks','A day with discrepancies','240 baseline trades with price, quantity, missing, and duplicate findings.'],['corrected','Corrected replacement files','New revisions restore the matched baseline and retain case history.'],['missing','Internal delivery only','On an empty scope, the broker remains outstanding and runs are blocked.'],['empty','Explicit empty broker file','A complete zero-row manifest produces legitimate missing-broker findings.'],['corrupt','Truncated broker delivery','The producer’s original checksum blocks the damaged file.'],['rejected','Invalid decimal record','A NaN quantity is rejected; the batch cannot become ready.']].map(([name,title,description])=><button key={name} disabled={busy} onClick={()=>loadDemo(name)}><span className="scenario-icon">{name==='corrected'?<CheckCheck size={20}/>:name==='breaks'?<GitCompareArrows size={20}/>:<FileText size={20}/>}</span><div><strong>{title}</strong><p>{description}</p></div><ArrowRight size={17}/></button>)}</div></Modal>}
 {uploadSide&&<UploadModal side={uploadSide} onClose={()=>setUploadSide(null)} onSuccess={()=>{setUploadSide(null);setSelectedInputs({});setNotice('Delivery registered. Validation will run in the background.')}}/>}
 {confirmRun&&<Modal title="Start a reconciliation" subtitle="These exact input revisions and rules will be pinned for this run." onClose={()=>setConfirmRun(false)}><div className="confirm-inputs">{[internal,broker].map(b=>b&&<div key={b.id}><span><FileCheck2 size={18}/><strong>{nice(b.side)} · revision {b.revision}</strong><Badge value={b.status}/></span><p>{b.filename}</p><code>{b.sha256}</code><small>Batch {b.id} · {fmt(b.accepted)} accepted records</small></div>)}</div><div className="rule-note"><ShieldCheck size={18}/><span>Execution v1 · exact quantity · absolute price tolerance 0.01<br/>8 stable partitions · account, instrument, side, and currency compared exactly</span></div>{demoMode&&<div className="demo-options"><label><input type="checkbox" checked={pauseNext} onChange={e=>setPauseNext(e.target.checked)}/>Pause first partition for recovery demonstration</label><label><input type="checkbox" checked={failNext} onChange={e=>setFailNext(e.target.checked)}/>Inject one transient failure in partition 2</label></div>}<div className="modal-actions"><button className="btn secondary" onClick={()=>setConfirmRun(false)}>Cancel</button><button className="btn primary" disabled={busy||!ready} onClick={launch}>{busy?<LoaderCircle size={15} className="spin"/>:<Play size={15}/>}Start run</button></div></Modal>}
 {batchDetail&&<Modal title={`${nice(batchDetail.side)} delivery · revision ${batchDetail.revision}`} subtitle={batchDetail.filename} onClose={()=>setBatchDetail(null)} wide><div className="batch-summary"><Badge value={batchDetail.status}/><span>{fmt(batchDetail.committed_accepted)} accepted</span><span>{fmt(batchDetail.committed_rejected)} rejected</span><span>{batchDetail.work.filter((w:Work)=>w.kind==='chunk'&&w.state==='SUCCEEDED').length} / {batchDetail.work.filter((w:Work)=>w.kind==='chunk').length} chunks committed</span><a className="text-link push" href={`/api/batches/${batchDetail.id}/source`}><ArrowDownToLine size={15}/>Original file</a></div><div className="checks-grid">{Object.entries(batchDetail.checks).map(([k,v])=><div key={k}>{v===true?<Check size={16}/>:v===false?<X size={16} className="red"/>:<Clock3 size={16}/>}<span>{nice(k)}</span><strong>{v===null?'Pending':v?'Passed':'Failed'}</strong></div>)}</div>{batchDetail.error&&<div className="alert error">{batchDetail.error}</div>}<div className="lineage-block"><small>INPUT SHA-256</small><code>{batchDetail.sha256}</code><small>BATCH / NORMALIZATION GENERATION</small><code>{batchDetail.id} / {batchDetail.generation}</code></div>{rejections.length>0&&<><h3>Rejected source records</h3>{rejections.map(r=><div className="rejection" key={r.id}><strong>Record {r.record_number}</strong>{r.errors.map((e:any,i:number)=><p key={i}><b>{e.field}</b> · {e.code}: {e.message}</p>)}<pre>{JSON.stringify(r.raw,null,2)}</pre></div>)}</>}<details><summary>{batchDetail.manifest_origin==='OPERATOR'?'Uploader declaration & file checks':'Producer manifest'}</summary><pre>{JSON.stringify(batchDetail.manifest,null,2)}</pre></details></Modal>}
 {caseId&&<div className="drawer-overlay" onMouseDown={e=>{if(e.target===e.currentTarget)setCaseId(null)}}><aside className="case-drawer" role="dialog" aria-modal="true" aria-label="Case investigation"><div className="drawer-head"><span className="eyebrow">CASE · BW-{short(caseId).toUpperCase()}</span><label className="investigator-select">Investigating as <select aria-label="Investigator identity" value={actor} onChange={e=>setActor(e.target.value)}><option>Alice</option><option>Bob</option></select></label><button className="icon-btn" aria-label="Close case" onClick={()=>setCaseId(null)}><X size={20}/></button></div>{caseDetail?<><div className="case-title"><h2>{caseDetail.reconciliation_ref}<span>{nice(caseDetail.family)}</span></h2><div><Badge value={caseDetail.workflow}/><Badge value={caseDetail.detection}/></div><p>Version {caseDetail.version} <i/> {caseDetail.assignee?`Assigned to ${caseDetail.assignee}`:'Unassigned'} <i/> Opened {time(caseDetail.created_at)}</p></div>{caseError&&<div className="alert error" role="alert"><TriangleAlert size={18}/><div>{caseError}<button className="text-link" onClick={async()=>{setCaseDetail(await api(`/cases/${caseId}`));setCaseError('')}}>Reload latest case</button></div></div>}<div className="evidence-heading"><h3>Comparison evidence</h3><select aria-label="Observation version" value={observation??''} onChange={e=>setObservation(e.target.value?Number(e.target.value):null)}><option value="">Latest finding evidence</option>{caseDetail.observations?.map((o,i)=><option key={o.run_id} value={i}>Run #{o.sequence} · {nice(o.detection)}</option>)}</select></div>{observation!==null&&caseDetail.observations?.[observation].evidence?.internal===undefined?<div className="readiness-note">{nice(caseDetail.observations![observation].detection)} in this run. Original finding evidence is retained below.</div>:null}<EvidenceView evidence={observation!==null&&caseDetail.observations?.[observation].evidence?.internal?caseDetail.observations[observation].evidence:caseDetail.evidence}/><div className="investigate-actions"><h3>Investigation</h3><div className="case-action-row"><button className="btn secondary" disabled={busy} onClick={()=>caseAct('claim')}><Users size={15}/>Claim as {actor}</button><button className="btn secondary" disabled={busy} onClick={()=>caseAct('review')}>Request review</button><button className="btn secondary" disabled={busy||caseDetail.detection!=='CLEARED'} onClick={()=>caseAct('resolve')}><CheckCheck size={15}/>Resolve</button></div><label className="note-label" htmlFor="note">Note or waiver reason</label><textarea id="note" placeholder="Record what you found, or explain an accepted difference…" value={note} onChange={e=>setNote(e.target.value)} maxLength={4000}/><div className="note-actions"><button className="btn secondary" disabled={busy||!note.trim()||caseDetail.detection!=='PRESENT'} onClick={()=>caseAct('waive')}>Waive for 24 hours</button><button className="btn primary" disabled={busy||!note.trim()} onClick={()=>caseAct('note')}><Plus size={15}/>Add note</button></div></div><div className="timeline"><h3>Audit timeline <span className="count-tag">{caseDetail.events?.length}</span></h3>{[...(caseDetail.events||[])].reverse().map(e=><div className="timeline-event" key={e.id}><span className={'timeline-dot '+(e.actor==='System'?'system':'')}>{e.actor==='System'?<RefreshCw size={12}/>:e.actor[0]}</span><div><strong>{e.actor} <span>{nice(e.kind)}</span></strong><p>{e.reason}</p><small>{time(e.created_at)}</small></div></div>)}</div></>:<Empty title="Loading investigation…"><LoaderCircle className="spin"/></Empty>}</aside></div>}
 {group&&<Modal title={`Comparison · ${group.reconciliation_ref}`} subtitle={`${nice(group.outcome)} · immutable run evidence`} onClose={()=>setGroup(null)} wide><EvidenceView evidence={group.evidence}/></Modal>}
 </div>
}

function EvidenceView({evidence:e}:{evidence:Evidence}){
 const a=e.internal[0]?.normalized,b=e.broker[0]?.normalized
 return <div className="evidence"><div className="table-scroll"><table className="evidence-table"><thead><tr><th>Compared field</th><th>Internal</th><th>Broker</th><th>Rule</th></tr></thead><tbody>{fields.map(field=>{const diff=e.differences.find(d=>d.field===field);const ambiguous=e.internal.length>1||e.broker.length>1;return <tr className={diff?'field-failed':''} key={field}><td>{fieldName(field)}</td><td className="mono">{ambiguous?'See source rows':a?.[field]||'—'}</td><td className="mono">{ambiguous?'See source rows':b?.[field]||'—'}</td><td><span className="rule-result">{diff?<X size={13}/>:a&&b&&!ambiguous?<Check size={13}/>:null}{field==='execution_price'?'≤ 0.01':'Exact'}</span></td></tr>})}</tbody></table></div>{e.notional_delta!==undefined&&<div className="notional"><span>Broker − internal notional</span><strong className="mono">{e.notional_currency} {e.notional_delta}</strong></div>}{e.duplicate_detail&&<div className="readiness-note"><Layers3 size={16}/><span>{Object.entries(e.duplicate_detail).map(([s,d])=>`${nice(s)}: ${nice(d)}`).join(' · ')}. All participating records are preserved.</span></div>}<h4>Source lineage</h4>{(['internal','broker'] as const).map(side=><div className="source-lineage" key={side}><div><strong>{nice(side)}</strong><span>{e[side].length} record{e[side].length!==1?'s':''}</span></div>{e[side].length?e[side].map(r=><details key={r.id}><summary><FileText size={14}/><span>{r.filename} <b>record {r.record_number}</b></span><ChevronDown size={14}/></summary><div className="source-values"><span>Revision {r.revision} · Batch {r.batch_id}</span><code>SHA-256 {r.sha256}</code><a href={`/api/batches/${r.batch_id}/source`} className="text-link"><ArrowDownToLine size={14}/>Download original file</a><h5>Original source fields</h5><pre>{JSON.stringify(r.raw,null,2)}</pre><h5>Normalized fields · equities-v1</h5><pre>{JSON.stringify(r.normalized,null,2)}</pre></div></details>):<p className="muted">No record in this complete input.</p>}</div>)}</div>
}

function UploadModal({side,onClose,onSuccess}:{side:string;onClose:()=>void;onSuccess:()=>void}){
 const [csv,setCsv]=useState<File|null>(null),[manifest,setManifest]=useState(''),[busy,setBusy]=useState(false),[error,setError]=useState('')
 async function submit(){if(!csv)return;setBusy(true);setError('');try{JSON.parse(manifest);const f=new FormData();f.set('file',csv);f.set('manifest',manifest);await api('/uploads',{method:'POST',headers:{'Idempotency-Key':crypto.randomUUID()},body:f});onSuccess()}catch(e){setError((e as Error).message)}finally{setBusy(false)}}
 return <Modal title={`Upload ${side.toLowerCase()} delivery`} subtitle="Supply the original CSV and the producing party’s sidecar manifest." onClose={onClose}><label className="upload-zone"><Upload size={28}/><strong>{csv?csv.name:'Choose a CSV file'}</strong><span>UTF-8 CSV · up to 50 MiB</span><input aria-label="CSV file" type="file" accept=".csv,text/csv" onChange={e=>setCsv(e.target.files?.[0]||null)}/></label><div className="manifest-label"><label htmlFor="manifest">Producer manifest</label><label className="text-link">Load JSON file<input aria-label="Manifest JSON file" className="hidden-input" type="file" accept=".json,application/json" onChange={async e=>{const file=e.target.files?.[0];if(file)setManifest(await file.text())}}/></label></div><textarea id="manifest" className="manifest-input mono" value={manifest} onChange={e=>setManifest(e.target.value)} placeholder={'{\n  "schema_version": 1,\n  "side": "'+side+'",\n  "complete": true,\n  …\n}'}/><p className="helper">Expected byte count, row count, checksum, and account coverage must come from the producer. A received file’s checksum alone cannot establish completeness.</p>{error&&<div className="alert error" role="alert">{error}</div>}<div className="modal-actions"><button className="btn secondary" onClick={onClose}>Cancel</button><button className="btn primary" disabled={!csv||!manifest||busy} onClick={submit}>{busy?<LoaderCircle size={16} className="spin"/>:<Upload size={16}/>}Upload & validate</button></div></Modal>
}


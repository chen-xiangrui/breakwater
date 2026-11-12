import {useEffect,useRef,type ReactNode} from 'react'
import {X} from 'lucide-react'

export default function Modal({title,subtitle,children,onClose,wide=false,busy=false}:{
  title:string;subtitle?:string;children:ReactNode;onClose:()=>void;wide?:boolean;busy?:boolean
}){
  const dialog=useRef<HTMLElement>(null),latest=useRef({onClose,busy})
  latest.current={onClose,busy}
  useEffect(()=>{
    const previous=document.activeElement as HTMLElement|null
    const element=dialog.current!
    element.querySelector<HTMLElement>('input,select,textarea,button')?.focus()
    const keydown=(event:KeyboardEvent)=>{
      const dialogs=document.querySelectorAll('.overlay .modal')
      if(dialogs[dialogs.length-1]!==element)return
      if(event.key==='Escape'){
        event.preventDefault()
        if(!latest.current.busy)latest.current.onClose()
      }
      if(event.key==='Tab'){
        const fields=Array.from(element.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled),select:not(:disabled),textarea:not(:disabled),a[href]')).filter(x=>x.offsetParent!==null)
        const first=fields[0],last=fields[fields.length-1]
        if(event.shiftKey&&document.activeElement===first){event.preventDefault();last?.focus()}
        else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first?.focus()}
      }
    }
    window.addEventListener('keydown',keydown)
    return()=>{window.removeEventListener('keydown',keydown);if(previous?.isConnected)previous.focus()}
  },[])
  return <div className="overlay" onMouseDown={event=>{if(event.target===event.currentTarget&&!busy)onClose()}}>
    <section ref={dialog} role="dialog" aria-modal="true" aria-label={title} className={'modal '+(wide?'wide':'')}>
      <div className="modal-head"><div><h2>{title}</h2>{subtitle&&<p>{subtitle}</p>}</div><button disabled={busy} aria-label="Close dialog" className="icon-btn" onClick={onClose}><X size={20}/></button></div>
      {children}
    </section>
  </div>
}

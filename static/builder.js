let reviewedInput = null;
const form = document.querySelector('#builder-form');
const status = document.querySelector('#builder-status');
const runLocal = document.querySelector('#run-local');
const download = document.querySelector('#export');
function input() { return Object.fromEntries(['prompt','kind','name','catalog','schema'].map(k=>[k,document.getElementById(k).value])); }
function add(parent,tag,text) { const node=document.createElement(tag); node.textContent=text; parent.append(node); return node; }
form.addEventListener('input',()=>{reviewedInput=null;download.disabled=true;runLocal.disabled=true;status.textContent='Inputs changed. Generate a fresh plan before export.';});
form.addEventListener('submit',async event=>{
 event.preventDefault();download.disabled=true;runLocal.disabled=true;reviewedInput=null;
 try {
 const data=input();const response=await fetch('/api/workflow-plan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const plan=await response.json();if(!response.ok)throw Error(plan.error);
 status.textContent=plan.status+' · Local checks only; workspace validation pending.';
 const section=document.querySelector('#plan');section.replaceChildren();add(section,'h2','Infrastructure and review');add(section,'pre',JSON.stringify(plan.infrastructure,null,2));
 for(const item of [...plan.blockers,...plan.checks,...plan.limitations])add(section,'p',item);
 const files=document.querySelector('#files');files.replaceChildren();for(const [path,content]of Object.entries(plan.files)){const detail=add(files,'details','');add(detail,'summary',path);add(detail,'pre',content);}
 if(plan.status!=='BLOCKED'){reviewedInput=data;download.disabled=false;runLocal.disabled=false;}
 }catch(error){status.textContent=error.message;}
});
download.addEventListener('click',async()=>{
 if(!reviewedInput)return;
 try{const response=await fetch('/api/workflow-export',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(reviewedInput)});if(!response.ok){const result=await response.json();throw Error(result.error);}const url=URL.createObjectURL(await response.blob());const link=document.createElement('a');link.href=url;link.download='synthetic-databricks-bundle.zip';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}catch(error){status.textContent=error.message;}
});

runLocal.addEventListener('click',async()=>{
 if(!reviewedInput)return;
 runLocal.disabled=true;
 const result=document.querySelector('#local-result');result.replaceChildren();add(result,'p','Executing synthetic workflow…');
 try{
 const response=await fetch('/api/workflow-run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...reviewedInput,scenario:document.querySelector('#local-scenario').value})});
 const run=await response.json();if(!response.ok)throw Error(run.error);
 result.replaceChildren();add(result,'h3',run.workflow.definition.name+' · '+run.workflow.status);
 for(const task of run.workflow.tasks)add(result,'p',task.key+' → '+task.status);
 add(result,'p','Detected: '+run.risk.status+' · '+run.risk.reasons.join(' '));
 add(result,'p','Plan SHA-256: '+run.workflow.plan_fingerprint);
 const lab=add(result,'a','Continue investigation and recovery in Delivery Lab →');lab.href='/delivery?run='+encodeURIComponent(run.id);
 add(result,'p','');const investigation=add(result,'a','Open linked Signal investigation →');investigation.href='/?incident='+encodeURIComponent('delivery-incident:'+run.id);
 }catch(error){result.replaceChildren();add(result,'p',error.message);}finally{runLocal.disabled=!reviewedInput;}
});

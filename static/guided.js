const el=id=>document.getElementById(id);
const labels=['Workflow ready','Execute workflow','Detect failure','Investigate impact','Request recovery','Approve recovery','Recover MFTS only','Verify consumers'];
const stories=[
 'A generated workflow is bound to its plan fingerprint. The executor will ingest a synthetic file, distribute records and verify consumers.',
 'The local worker validates five records. Kafka accepts all five; the injected transfer timeout prevents MFTS delivery. Verification stops at the dependency gate.',
 'The read-only advisor detects the missed deadline from persisted channel results and the virtual clock. This is an observed failure, not a prewritten status.',
 'Lineage identifies the affected Treasury dashboard and Operations report. Kafka evidence is complete, so the supported recommendation is MFTS-only recovery.',
 'operator-demo requests recovery. The proposal records the exact action and evidence fingerprint; changing evidence makes the approval stale.',
 'architect-demo approves the current proposal. These are simulated actors demonstrating separation of duties, not enterprise authentication.',
 'The approved worker retries only MFTS. Five receiver artifacts are written while Kafka remains at five accepted records. Consumers still need verification.',
 'Consumer payloads are compared with canonical records. Both reports turn green and all workflow tasks succeed. The original missed deadline remains recorded in the audit history.'
];
let run=null,index=0,playing=false,busy=false,timer=null;
function node(tag,text){const n=document.createElement(tag);n.textContent=text;return n;}
function stop(){playing=false;clearTimeout(timer);timer=null;}
function controls(){el('guide-start').disabled=busy;el('guide-pause').disabled=!run||busy||run.guided_demo.length===8&&index===7;el('guide-pause').textContent=playing?'Pause':'Resume';el('guide-back').disabled=busy||!run||index===0;el('guide-next').disabled=busy||!run||index===7;}
function render(){if(!run){controls();return;}const item=run.guided_demo[index],v=item.view,root='delivery:'+v.id;
 el('guide-steps').replaceChildren(...labels.map((label,i)=>{const n=node('li',(i+1)+'. '+label);if(i===index)n.setAttribute('aria-current','step');return n;}));
 el('guide-story').replaceChildren(node('h2','Step '+(index+1)+' of 8 · '+item.label),node('p',stories[index]));
 const evidence=el('guide-evidence');evidence.replaceChildren(node('h3','Evidence at this step'));
 const facts={workflow:v.workflow.status,tasks:v.workflow.tasks.map(t=>t.key+': '+t.status),risk:v.risk.status,kafka_writes:v.broker.length,mfts_artifacts:Object.keys(v.receiver).length,consumers:v.consumers,proposal:v.proposal,plan_fingerprint:v.workflow.plan_fingerprint,evidence_fingerprint:v.risk.fingerprint,virtual_seconds:v.clock};
 const summary=node('div','');summary.className='pipeline-steps';for(const text of ['Workflow: '+v.workflow.status,'Kafka records: '+v.broker.length,'MFTS artifacts: '+Object.keys(v.receiver).length,'Verified reports: '+Object.keys(v.consumers).length,'Approval: '+(v.proposal?.status||'Not requested')])summary.append(node('span',text));evidence.append(summary);evidence.append(node('p',v.workflow.tasks.map(t=>t.key+' → '+t.status).join(' · ')));const payload=node('details','');payload.append(node('summary','Inspect fingerprints and full step evidence'),node('pre',JSON.stringify(facts,null,2)));evidence.append(payload);const audit=node('details','');audit.append(node('summary','Audit evidence ('+v.audit.length+' events)'),node('pre',JSON.stringify(v.audit.slice(-8),null,2)));evidence.append(audit);
 const assets=[{id:root,name:'Canonical treasury data',type:'DATASET'},...['kafka','mfts'].map(c=>({id:root+':'+c,name:'Local '+c.toUpperCase(),type:'CHANNEL'}))];
 assets.push({id:root+':treasury',name:'Treasury dashboard',type:'REPORT'},{id:root+':operations',name:'Operations report',type:'REPORT'});
 el('guide-graph').replaceChildren(lineageView(assets[0],v.projection,assets,'Lineage · '+item.label));
 const links=el('guide-links');links.replaceChildren();for(const [text,url]of [['Open current Delivery Lab run','/delivery?run='+run.id],['Open current investigation','/?incident='+encodeURIComponent('delivery-incident:'+run.id)],['Download full evidence','/api/delivery/evidence/'+run.id]]){const a=node('a',text);a.href=url;links.append(a);}
 el('guide-notice').textContent=index===7?'Demo complete. All tasks and consumers verified. Review earlier steps without changing the run.':(playing?'Playing · next step in 7 seconds.':'Paused · inspect evidence or move to the next step.')+(index<run.guided_demo.length-1?' Showing a historical snapshot; links open current state.':'');controls();}
function schedule(){clearTimeout(timer);if(playing&&index<7)timer=setTimeout(advance,7000);}
async function post(data){const r=await fetch('/api/guided-demo',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const body=await r.json();if(!r.ok)throw Error(body.error);return body;}
async function advance(){if(busy||!run||index===7)return;busy=true;controls();try{if(index<run.guided_demo.length-1)index++;else{run=await post({command:'next',id:run.id,expected_step:run.guided_demo.length});index=run.guided_demo.length-1;}if(index===7)stop();render();}catch(e){stop();el('guide-notice').textContent=e.message+' Resume to retry the current step.';}finally{busy=false;controls();schedule();}}
el('guide-start').onclick=async()=>{if(busy)return;stop();busy=true;controls();try{run=await post({command:'start'});index=0;playing=true;history.replaceState(null,'','/demo?run='+encodeURIComponent(run.id));render();}catch(e){el('guide-notice').textContent=e.message;}finally{busy=false;controls();schedule();}};
el('guide-pause').onclick=()=>{if(playing)stop();else playing=true;render();schedule();};
el('guide-next').onclick=()=>{stop();advance();};el('guide-back').onclick=()=>{stop();if(index>0)index--;render();};
window.addEventListener('pagehide',stop);
(async()=>{const id=new URLSearchParams(location.search).get('run');if(!id)return;try{const response=await fetch('/api/delivery/evidence/'+encodeURIComponent(id));const value=await response.json();if(!response.ok||!value.guided_demo?.length)throw Error('Guided run not found');run=value;index=run.guided_demo.length-1;render();}catch(e){el('guide-notice').textContent=e.message;}})();

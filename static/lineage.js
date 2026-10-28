/* Pure graph model shared by the SVG view and Node tests. */
(function(root){
  function model(asset, investigation, assets){
    const edges=investigation.dependencies||[], levels=new Map([[asset.id,0]]), queue=[asset.id];
    while(queue.length){const id=queue.shift();for(const edge of edges){if(edge.source===id&&!levels.has(edge.target)){levels.set(edge.target,levels.get(id)+1);queue.push(edge.target);}}}
    const unknown=(investigation.hypotheses||[]).some(h=>h.assessment==='insufficient evidence');
    const healthy=investigation.recovery_verified;
    return {nodes:[...levels].map(([id,level])=>{const a=assets.find(a=>a.id===id)||{id,name:id};const primary=id===asset.id;const refreshed=healthy&&investigation.downstream_verified?.[id]?.simulated===true;const override=investigation.node_states?.[id];return {id,label:a.name||id,type:a.type||(a.table?'TABLE':'REPORT'),level,status:override?override.status:primary?(healthy?'healthy':unknown?'unknown':'failed'):(refreshed?'healthy':healthy?'pending':'affected'),caption:override?override.caption:primary?(healthy?'Checks passed':unknown?'Health unknown':'Incident active'):(refreshed?'Refresh passed (simulated)':healthy?'Refresh / verify pending':'Potentially affected')};}),edges:edges.filter(e=>levels.has(e.source)&&levels.has(e.target))};
  }
  root.Lineage={model};
})(typeof module==='object'?module.exports:window);

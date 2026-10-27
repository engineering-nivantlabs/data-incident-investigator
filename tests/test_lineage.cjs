const assert=require('node:assert/strict');
const {Lineage}=require('../static/lineage.js');
const asset={id:'a',name:'Source',table:'main.test.a'};
const assets=[asset,{id:'b',name:'Metrics',type:'TABLE'},{id:'c',name:'Report',type:'REPORT'}];
const base={dependencies:[{source:'a',target:'b'},{source:'b',target:'c'},{source:'c',target:'a'},{source:'outside',target:'secret'}],hypotheses:[{assessment:'confirmed'}],recovery_verified:false};
let g=Lineage.model(asset,base,assets);
assert.equal(g.nodes.length,3);assert.equal(g.edges.length,3);assert.equal(g.nodes[0].status,'failed');assert.equal(g.nodes[2].level,2);assert.equal(g.nodes[2].status,'affected');
g=Lineage.model(asset,{...base,recovery_verified:true,hypotheses:[]},assets);
assert.equal(g.nodes[0].status,'healthy');assert.equal(g.nodes[2].status,'pending');assert.equal(g.nodes[2].caption,'Refresh / verify pending');
g=Lineage.model(asset,{...base,hypotheses:[{assessment:'insufficient evidence'}]},assets);assert.equal(g.nodes[0].status,'unknown');
assert.equal(Lineage.model(asset,{hypotheses:[]},assets).nodes.length,1);
console.log('Lineage graph tests passed: cycles, exposure, recovery, unknown status, isolated assets.');

g=Lineage.model(asset,{...base,recovery_verified:true,downstream_verified:{b:{simulated:true},c:{simulated:true}}},assets);
assert.ok(g.nodes.every(n=>n.status==='healthy'));
assert.equal(g.nodes[2].caption,'Refresh passed (simulated)');
g=Lineage.model(asset,{...base,recovery_verified:false,downstream_verified:{c:{simulated:true}}},assets);assert.equal(g.nodes[2].status,'affected');

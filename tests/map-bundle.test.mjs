import {test} from 'node:test';import assert from 'node:assert/strict';import {loadMapBundle} from '../api/map-bundle.js';
import {loadMapOverview,mapPaths} from '../web/expressive-map.js';
test('shipped self-hosted OSM overview preserves provenance and fits the bounded mobile renderer',()=>{
 const bundle=loadMapBundle();assert.equal(bundle.license,'ODbL-1.0');assert.match(bundle.attribution,/OpenStreetMap/);assert.match(bundle.source_sha256,/^[0-9a-f]{64}$/);assert.ok(bundle.overview_only);const paths=mapPaths(bundle);assert.ok(paths.length>100);assert.ok(paths.every(path=>!path.includes('NaN')));
});
test('overview loads share one request and retry after a failed request',async()=>{
 let calls=0;const bundle={type:'LineString',coordinates:[[0,0],[1,1]]},request=async()=>{calls++;return bundle;};
 const [a,b]=await Promise.all([loadMapOverview('test:shared',request),loadMapOverview('test:shared',request)]);assert.equal(calls,1);assert.equal(a,b);assert.equal(a.paths.length,1);
 await assert.rejects(loadMapOverview('test:retry',async()=>{throw Error('offline');}));await loadMapOverview('test:retry',request);assert.equal(calls,2);
});

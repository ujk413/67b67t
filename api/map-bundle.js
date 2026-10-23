import {readFileSync} from 'node:fs';
/** Static public geography only; location coordinates never reach this loader. */
export function loadMapBundle(file=new URL('./map-data/osm-overview.geojson',import.meta.url)){
 let raw;try{raw=readFileSync(file,'utf8');}catch(error){if(error.code==='ENOENT')return null;throw error;}
 if(Buffer.byteLength(raw)>2*1024*1024)throw Error('map_bundle_too_large');
 const bundle=JSON.parse(raw);if(bundle.type!=='FeatureCollection'||!Array.isArray(bundle.features)||bundle.features.length>20000||bundle.license!=='ODbL-1.0')throw Error('invalid_map_bundle');
 let points=0;
 for(const feature of bundle.features){if(feature?.geometry?.type!=='MultiLineString')throw Error('invalid_map_bundle');for(const line of feature.geometry.coordinates){if(!Array.isArray(line))throw Error('invalid_map_bundle');for(const [longitude,latitude] of line){if(++points>50000||!Number.isFinite(longitude)||!Number.isFinite(latitude)||Math.abs(longitude)>180||Math.abs(latitude)>90)throw Error('invalid_map_bundle');}}}
 return bundle;
}

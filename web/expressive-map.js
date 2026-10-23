/** Local GeoJSON -> SVG paths. All coordinates stay in memory on the client. */
export function projectPoint(longitude,latitude){
 if(!Number.isFinite(longitude)||!Number.isFinite(latitude)||Math.abs(longitude)>180||Math.abs(latitude)>90)throw Error('invalid_map_coordinate');
 return [longitude+180,90-latitude];
}
export function mapPaths(bundle,maxPoints=50000){
 if(!bundle||typeof bundle!=='object')throw Error('invalid_map_bundle');
 const features=bundle.type==='FeatureCollection'?bundle.features:bundle.type==='Feature'?[bundle]:[{geometry:bundle}];
 if(!Array.isArray(features)||features.length>20000)throw Error('invalid_map_bundle');
 let total=0;const paths=[];
 const ring=coords=>{if(!Array.isArray(coords)||coords.length>maxPoints)throw Error('invalid_map_bundle');return coords.map((point,i)=>{if(++total>maxPoints||!Array.isArray(point)||point.length<2)throw Error('invalid_map_bundle');const [x,y]=projectPoint(Number(point[0]),Number(point[1]));return`${i?'L':'M'}${x.toFixed(3)} ${y.toFixed(3)}`;}).join('');};
 for(const feature of features){const geometry=feature?.geometry;if(!geometry)continue;const {type,coordinates}=geometry;if(type==='LineString')paths.push(ring(coordinates));else if(type==='MultiLineString'||type==='Polygon')for(const line of coordinates)paths.push(ring(line));else if(type==='MultiPolygon')for(const polygon of coordinates)for(const line of polygon)paths.push(ring(line));}
 return paths;
}
export function mapViewport(latitude,longitude,span=12){const [x,y]=projectPoint(longitude,latitude),width=Math.min(360,span*2),height=Math.min(180,span);return `${Math.max(0,Math.min(360-width,x-width/2))} ${Math.max(0,Math.min(180-height,y-height/2))} ${width} ${height}`;}
const overviews=new Map();
export function loadMapOverview(origin,fetchBundle){
 if(overviews.has(origin))return overviews.get(origin);
 const pending=Promise.resolve().then(fetchBundle).then(bundle=>({paths:mapPaths(bundle),attribution:bundle.attribution||'© OpenStreetMap contributors',license_url:'https://opendatacommons.org/licenses/odbl/1-0/',overview_only:bundle.overview_only===true})).catch(error=>{if(overviews.get(origin)===pending)overviews.delete(origin);throw error;});
 overviews.set(origin,pending);while(overviews.size>4)overviews.delete(overviews.keys().next().value);return pending;
}

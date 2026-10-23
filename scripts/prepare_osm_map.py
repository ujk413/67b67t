"""Build a small self-hosted OSM coastline overview (no runtime map provider).

Install build-only dependencies: pip install pyshp shapely
Input: osmdata.openstreetmap.de simplified-land-polygons-complete-3857.zip
The derivative data remains under ODbL1.0; provenance is embedded in GeoJSON.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import zipfile

import shapefile
from shapely.geometry import shape

SOURCE='https://osmdata.openstreetmap.de/download/simplified-land-polygons-complete-3857.zip'
ROOT=Path(__file__).resolve().parents[1]

def prepare(archive_path,output):
    with zipfile.ZipFile(archive_path) as archive:
        members=archive.namelist()
        parts={ext:archive.open(next(name for name in members if name.endswith('.'+ext))) for ext in ('shp','shx','dbf')}
        reader=shapefile.Reader(**parts)
        # An overview, not a street atlas: drop islands smaller than20km² and
        # increase simplification until mobile's bounded renderer accepts it.
        geometries=[]
        for item in reader.iterShapes():
            polygon=shape(item.__geo_interface__)
            if polygon.area>=20_000_000:geometries.append(polygon)
        tolerance=2000
        while True:
            lines=[];count=0
            for geometry in geometries:
                simplified=geometry.simplify(tolerance,preserve_topology=True)
                polygons=list(simplified.geoms) if simplified.geom_type=='MultiPolygon' else [simplified]
                for polygon in polygons:
                    if polygon.is_empty:continue
                    for ring in [polygon.exterior,*polygon.interiors]:
                        line=[]
                        for x,y in ring.coords:
                            longitude=max(-180,min(180,math.degrees(x/6378137)))
                            latitude=math.degrees(2*math.atan(math.exp(y/6378137))-math.pi/2)
                            point=[round(longitude,4),round(latitude,4)]
                            if line and abs(point[0]-line[-1][0])>180:
                                if len(line)>1:lines.append(line);count+=len(line)
                                line=[]
                            line.append(point)
                        if len(line)>1:lines.append(line);count+=len(line)
            if count<=48000:break
            tolerance*=1.5
        bundle={'type':'FeatureCollection','attribution':'© OpenStreetMap contributors',
            'license':'ODbL-1.0','license_url':'https://opendatacommons.org/licenses/odbl/1-0/',
            'source_url':SOURCE,'source_sha256':hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            'overview_only':True,'simplification_metres':tolerance,
            'features':[{'type':'Feature','properties':{},'geometry':{'type':'MultiLineString','coordinates':lines}}]}
        output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps(bundle,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8',newline='\n')
        print(f'OSM overview: {count} points, {len(lines)} lines, {output.stat().st_size} bytes')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('archive',type=Path);parser.add_argument('--output',type=Path,default=ROOT/'api/map-data/osm-overview.geojson')
    args=parser.parse_args();prepare(args.archive,args.output)

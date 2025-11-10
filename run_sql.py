import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pathlib import Path
import os
import sys
import time

# --- КОНФІГУРАЦІЯ ТА ШЛЯХІ ---

CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from config import DB_CONFIG, TABLE_NAMES, GEOM_PARAMS

FILE_PATH = CURRENT_DIR / "dataset" / "ukraine_border.geojson" 
GRID_SIZE = GEOM_PARAMS['SQUARE_SIZE_M']
SECTOR_RADIUS = GEOM_PARAMS['SECTOR_RADIUS_M']
CLEANUP_BUFFER = GEOM_PARAMS['CLEANUP_BUFFER_DEG']

# --- SQL БЛОК A: ФУНКЦІЯ (ПОВИННА ВИКОНУВАТИСЯ ОДНИМ ЗАПИТОМ) ---

SQL_FUNCTION = f"""
CREATE OR REPLACE FUNCTION ST_Sector_Fixed(
    center GEOMETRY, 
    radius NUMERIC, 
    azimuth_center NUMERIC, 
    angle_width NUMERIC
)
RETURNS GEOMETRY AS $$
DECLARE
    angle_start NUMERIC := azimuth_center - angle_width / 2;
    angle_end NUMERIC := azimuth_center + angle_width / 2;
    num_points INTEGER := 16; 
    points GEOMETRY[];
    i INTEGER;
BEGIN
    points := ARRAY[center]; 
    FOR i IN 0..num_points LOOP
        points := array_append(points, 
            ST_Project(
                center::geography, 
                radius, 
                radians(angle_start + (angle_end - angle_start) * i / num_points)
            )::geometry
        );
    END LOOP;
    points := array_append(points, center);
    RETURN ST_SetSRID(ST_MakePolygon(ST_MakeLine(points)), ST_SRID(center));
END;
$$ LANGUAGE plpgsql;
"""

# --- SQL БЛОК B: ОСНОВНІ КОМАНДИ (РОЗДІЛЯЮТЬСЯ КРАПКОЮ З КОМОЮ) ---

SQL_COMMANDS = f"""
DROP TABLE IF EXISTS {TABLE_NAMES['CENTER']} CASCADE;
DROP TABLE IF EXISTS {TABLE_NAMES['RAW_UNION_SAFE']} CASCADE;
DROP TABLE IF EXISTS {TABLE_NAMES['CLEAN_BORDER']} CASCADE;
DROP TABLE IF EXISTS {TABLE_NAMES['GRID']} CASCADE;
DROP TABLE IF EXISTS {TABLE_NAMES['VERTICES']} CASCADE;
DROP TABLE IF EXISTS {TABLE_NAMES['SECTORS']} CASCADE;
DROP TABLE IF EXISTS sector_intersections_full CASCADE;
DROP TABLE IF EXISTS sector_intersections_half CASCADE;
DROP TABLE IF EXISTS buffer_step CASCADE;


-- I. BORDER CLEANUP AND GEOMETRY BASE
CREATE TABLE {TABLE_NAMES['RAW_UNION_SAFE']} AS
SELECT ST_Union(ST_MakeValid(geometry)) AS geom
FROM {TABLE_NAMES['BORDER']};

CREATE TABLE {TABLE_NAMES['CENTER']} AS
SELECT 
    ST_X(ST_Centroid(ST_Union(ST_MakeValid(geom)))) AS center_lon,
    ST_Y(ST_Centroid(ST_Union(ST_MakeValid(geom)))) AS center_lat
FROM {TABLE_NAMES['RAW_UNION_SAFE']};

CREATE TABLE buffer_step AS
SELECT 
    ST_Buffer(
        ST_Buffer(geom, {CLEANUP_BUFFER}, 'join=mitre endcap=flat'), 
        -{CLEANUP_BUFFER}, 
        'join=mitre endcap=flat'
    ) AS cleaned_geom
FROM {TABLE_NAMES['RAW_UNION_SAFE']};

CREATE TABLE {TABLE_NAMES['CLEAN_BORDER']} AS
WITH FinalDump AS (
    SELECT (ST_Dump(cleaned_geom)).geom AS geom
    FROM buffer_step
)
SELECT geom 
FROM FinalDump
ORDER BY ST_Area(geom) DESC 
LIMIT 1;

DROP TABLE buffer_step;

-- II. GRID AND VERTICES CREATION
CREATE TABLE {TABLE_NAMES['GRID']} AS
SELECT
    (ST_SquareGrid({GRID_SIZE}, ST_SetSRID(ST_Extent(ST_Transform(geom, 3857)), 3857))).*
FROM {TABLE_NAMES['CLEAN_BORDER']};

DELETE FROM {TABLE_NAMES['GRID']}
WHERE NOT ST_Intersects(geom, (
    SELECT ST_Transform(geom, 3857) 
    FROM {TABLE_NAMES['CLEAN_BORDER']}
));

CREATE TABLE {TABLE_NAMES['VERTICES']} AS
WITH DumpedPoints AS (
    SELECT 
        ug.i AS i_grid_id, 
        ug.j AS j_grid_id,
        (ug.i || '_' || ug.j) AS grid_cell_name, 
        (ST_DumpPoints(ug.geom)).geom AS vertex_point_3857
    FROM {TABLE_NAMES['GRID']} ug
)
SELECT
    dp.grid_cell_name, 
    dp.vertex_point_3857,
    ST_Transform(dp.vertex_point_3857, 4326) AS vertex_point 
FROM DumpedPoints dp;

ALTER TABLE {TABLE_NAMES['VERTICES']}
ADD COLUMN id SERIAL PRIMARY KEY;

-- Indexing
CREATE INDEX idx_clean_border_geom ON {TABLE_NAMES['CLEAN_BORDER']} USING GIST (geom);
CREATE INDEX idx_ukraine_grid_geom ON {TABLE_NAMES['GRID']} USING GIST (geom);
CREATE INDEX idx_vertices_geom ON {TABLE_NAMES['VERTICES']} USING GIST (vertex_point);


-- III. SECTOR CREATION
CREATE TABLE {TABLE_NAMES['SECTORS']} AS
WITH SectorData AS (
    SELECT
        id AS vertex_id,
        vertex_point AS center_point_4326, 
        azimuth
    FROM {TABLE_NAMES['VERTICES']},
    unnest(ARRAY[0, 120, 240]) AS azimuth
)
SELECT
    sd.vertex_id,
    sd.azimuth,
    ST_Sector_Fixed(
        sd.center_point_4326, 
        {SECTOR_RADIUS}, 
        sd.azimuth,        
        60                  
    ) AS sector_geom
FROM SectorData sd;

CREATE INDEX idx_all_sectors_geom ON {TABLE_NAMES['SECTORS']} USING GIST (sector_geom);

-- IV. FULL INTERSECTION ANALYSIS
CREATE TABLE sector_intersections_full AS
SELECT
    s.vertex_id AS sector_source_vertex_id, 
    s.azimuth,                               
    v.id AS intersecting_vertex_id         
FROM {TABLE_NAMES['SECTORS']} s 
JOIN {TABLE_NAMES['VERTICES']} v 
ON ST_Intersects(s.sector_geom, v.vertex_point);

CREATE INDEX idx_intersections_full_source_id ON sector_intersections_full USING BTREE (sector_source_vertex_id);
"""

# --- EXECUTION FUNCTION ---

def run_analysis_pipeline():
    
    if not FILE_PATH.exists():
        print(f"Error: GeoJSON file {os.path.basename(FILE_PATH)} not found in 'data/' directory.")
        return

    try:
        engine_string = (
            f"postgresql://{DB_CONFIG['USER']}:{DB_CONFIG['PASSWORD']}@"
            f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['NAME']}"
        )
        engine = create_engine(engine_string)
    except Exception as e:
        print(f"Critical Error: Could not establish DB connection. Check config.py. Error: {e}")
        return

    # 1. Import GeoJSON 
    try:
        print(f"1. Importing GeoJSON ({os.path.basename(FILE_PATH)}) to PostGIS...")
        gdf = gpd.read_file(FILE_PATH)
        gdf.to_postgis(TABLE_NAMES['BORDER'], engine, if_exists='replace', index=False)
        print("   Import successful.")
    except Exception as e:
        print(f"Critical Error: Failed to import GeoJSON. Error: {e}")
        return

    # 2. Execute all SQL commands
    print("\n2. Starting full SQL analysis pipeline (This step takes time)...")
    start_time = time.time()
    
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        
        # --- A. ВИКОНАННЯ ФУНКЦІЇ PL/pgSQL (один блок) ---
        print("   Executing PL/pgSQL function...")
        cursor.execute(SQL_FUNCTION)
        conn.commit()
        
        # --- B. ВИКОНАННЯ ОСНОВНИХ ЗАПИТІВ ---
        statements = [stmt.strip() for stmt in SQL_COMMANDS.split(';') if stmt.strip()]
        
        for i, stmt in enumerate(statements):
            cursor.execute(stmt)
            if not stmt.startswith("CREATE OR REPLACE FUNCTION"):
                 print(f"   Executed query {i+1}/{len(statements)}")
            
        conn.commit()
        
        end_time = time.time()
        print(f"\nAnalysis completed successfully in {end_time - start_time:.2f} seconds.")

    except ProgrammingError as e:
        print(f"\nCritical SQL Error. Analysis stopped. Error: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    run_analysis_pipeline()
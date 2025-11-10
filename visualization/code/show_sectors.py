import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path
import os
import pandas as pd
import folium
import matplotlib.pyplot as plt
import sys

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent 
sys.path.append(str(PROJECT_ROOT))

from config import DB_CONFIG, TABLE_NAMES

CLEAN_BORDER_TABLE = TABLE_NAMES['CLEAN_BORDER']
GRID_TABLE = TABLE_NAMES['GRID']
SECTORS_TABLE = TABLE_NAMES['SECTORS']

GEOM_COLUMN = 'geom'
OUTPUT_SUBDIR = PROJECT_ROOT.joinpath('visualization', 'output', 'squares_sectors')
TARGET_SECTOR_PERCENT = 5 

def setup_db_engine():
    engine_string = (
        f"postgresql://{DB_CONFIG['USER']}:{DB_CONFIG['PASSWORD']}@"
        f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['NAME']}"
    )
    return create_engine(engine_string)

def create_output_path(filename):
    OUTPUT_SUBDIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_SUBDIR / filename

def load_data(engine):
    
    sectors_query = f"SELECT * FROM {SECTORS_TABLE} TABLESAMPLE SYSTEM ({TARGET_SECTOR_PERCENT})"
    gdf_sectors = gpd.read_postgis(
        sectors_query, 
        engine, 
        geom_col='sector_geom'
    ).to_crs(epsg=4326)
    
    grid_query = f"SELECT * FROM {GRID_TABLE}"
    gdf_grid = gpd.read_postgis(
        grid_query, 
        engine, 
        geom_col='geom', 
        crs=3857
    ).to_crs(epsg=4326)
    
    gdf_border = gpd.read_postgis(f"SELECT {GEOM_COLUMN} FROM {CLEAN_BORDER_TABLE}", engine, geom_col=GEOM_COLUMN).to_crs(epsg=4326)
    
    if gdf_grid.empty:
        return None, None, None, None, None

    center_lon = (gdf_grid.total_bounds[0] + gdf_grid.total_bounds[2]) / 2
    center_lat = (gdf_grid.total_bounds[1] + gdf_grid.total_bounds[3]) / 2

    return gdf_border, gdf_grid, gdf_sectors, center_lat, center_lon

def visualize_with_folium(gdf_border, gdf_grid, gdf_sectors, center_lat, center_lon):
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6, 
        tiles='cartodbpositron'
    )
    
    folium.GeoJson(
        gdf_border.to_json(),
        name='1. Кордон України',
        style_function=lambda x: {'color': 'black', 'weight': 3, 'fillOpacity': 0.0}
    ).add_to(m)
    
    folium.GeoJson(
        gdf_grid.to_json(),
        name=f'2. Сітка ({len(gdf_grid)} шт.)',
        style_function=lambda x: {'fillColor': 'none', 'color': '#777777', 'weight': 0.5, 'fillOpacity': 0.0}
    ).add_to(m)

    folium.GeoJson(
        gdf_sectors.to_json(),
        name=f'3. Сектори ({len(gdf_sectors)} шт.)',
        style_function=lambda x: {'fillColor': 'red', 'color': 'darkred', 'weight': 0.1, 'fillOpacity': 0.15}
    ).add_to(m)
    
    folium.LayerControl().add_to(m)

    map_file = create_output_path("ukraine_10k_sectors_folium.html")
    m.save(map_file)
    print(f"Folium HTML map saved successfully: {os.path.abspath(map_file)}")

def visualize_with_matplotlib(gdf_border, gdf_grid, gdf_sectors):
    
    minx, miny, maxx, maxy = gdf_border.total_bounds
    fig, ax = plt.subplots(1, 1, figsize=(15, 15))
    
    gdf_sectors.plot(
        ax=ax,
        edgecolor='#7D0000',
        facecolor='red',
        linewidth=0.1,
        alpha=0.15, 
        zorder=1
    )

    gdf_grid.plot(
        ax=ax, 
        edgecolor='#777777', 
        facecolor='none', 
        linewidth=0.5,
        alpha=1.0, 
        zorder=2 
    )
    
    gdf_border.plot(
        ax=ax, 
        edgecolor='black', 
        facecolor='none',
        linewidth=3,
        zorder=3 
    )
    
    ax.set_title(f"Фінальна Карта: Сітка та Сектори (Matplotlib)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect('equal')
    
    ax.set_xlim(minx - 0.1, maxx + 0.1) 
    ax.set_ylim(miny - 0.1, maxy + 0.1)
    
    image_file = create_output_path("ukraine_10k_sectors_matplotlib.png")
    plt.savefig(image_file, dpi=200)
    plt.close(fig)
    print(f"Matplotlib PNG saved: {os.path.abspath(image_file)}")

def visualize_final_map():
    engine = setup_db_engine()
    
    gdf_border, gdf_grid, gdf_sectors, center_lat, center_lon = load_data(engine)

    if gdf_border is None:
        return

    visualize_with_folium(gdf_border, gdf_grid, gdf_sectors, center_lat, center_lon)
    visualize_with_matplotlib(gdf_border, gdf_grid, gdf_sectors)


if __name__ == "__main__":
    visualize_final_map()
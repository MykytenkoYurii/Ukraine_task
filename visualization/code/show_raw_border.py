import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path
import folium
import os
import pandas as pd
import matplotlib.pyplot as plt
import sys

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent 
sys.path.append(str(PROJECT_ROOT))

from config import DB_CONFIG, TABLE_NAMES

RAW_UNION_TABLE = TABLE_NAMES['RAW_UNION_SAFE']
CENTER_TABLE = TABLE_NAMES['CENTER']
GEOM_COLUMN = 'geom'

OUTPUT_DIR_FINAL = PROJECT_ROOT.joinpath('visualization', 'output', 'border')


def setup_db_engine():
    engine_string = (
        f"postgresql://{DB_CONFIG['USER']}:{DB_CONFIG['PASSWORD']}@"
        f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['NAME']}"
    )
    return create_engine(engine_string)


def create_output_path(filename):
    OUTPUT_DIR_FINAL.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR_FINAL / filename


def load_data(engine): 
    
    gdf_raw_border = gpd.read_postgis(
        f"SELECT {GEOM_COLUMN} FROM {RAW_UNION_TABLE}", 
        engine, 
        geom_col=GEOM_COLUMN
    ).to_crs(epsg=4326) # Забезпечуємо WGS84 для коректної візуалізації
    
    df_center = pd.read_sql(f"SELECT center_lon, center_lat FROM {CENTER_TABLE}", engine)
    center_lat = df_center.iloc[0]['center_lat']
    center_lon = df_center.iloc[0]['center_lon']

    return gdf_raw_border, center_lat, center_lon


def visualize_with_folium(gdf_raw_border, center_lat, center_lon):
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles='cartodbpositron' 
    )

    folium.GeoJson(
        gdf_raw_border.to_json(),
        name='Raw Border',
        style_function=lambda x: {
            'fillColor': '#FF5733',
            'color': '#C70039', 
            'weight': 3,
            'fillOpacity': 0.4
        }
    ).add_to(m)
    
    map_file = create_output_path("ukraine_raw_border_folium_map.html")
    m.save(map_file)
    print(f"Folium map saved: {os.path.abspath(map_file)}")


def visualize_with_matplotlib(gdf_raw_border):

    minx, miny, maxx, maxy = gdf_raw_border.total_bounds
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    
    gdf_raw_border.plot(
        ax=ax, 
        edgecolor='#C70039', 
        facecolor='#FF5733', 
        linewidth=1,
        alpha=0.7
    )
    
    ax.set_title("Сирий (Неочищений) Контур України")
    ax.set_aspect('equal')
    ax.set_xlim(minx - 0.1, maxx + 0.1) 
    ax.set_ylim(miny - 0.1, maxy + 0.1)
    
    image_file = create_output_path("ukraine_raw_border_matplotlib.png")
    plt.savefig(image_file, dpi=300)
    plt.close(fig)
    print(f"MPL png saved: {os.path.abspath(image_file)}")


def visualize_raw_border():
    engine = setup_db_engine()
    gdf_raw_border, center_lat, center_lon = load_data(engine)

    visualize_with_folium(gdf_raw_border, center_lat, center_lon)
    visualize_with_matplotlib(gdf_raw_border)
    
    print("\nVisualization completed successfully.")


if __name__ == "__main__":
    visualize_raw_border()
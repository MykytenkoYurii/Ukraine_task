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

CLEAN_BORDER_TABLE = TABLE_NAMES['CLEAN_BORDER']
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
    
    gdf_clean_border = gpd.read_postgis(
        f"SELECT {GEOM_COLUMN} FROM {CLEAN_BORDER_TABLE}", 
        engine, 
        geom_col=GEOM_COLUMN
    )
    
    df_center = pd.read_sql(f"SELECT center_lon, center_lat FROM {CENTER_TABLE}", engine)
    center_lat = df_center.iloc[0]['center_lat']
    center_lon = df_center.iloc[0]['center_lon']

    return gdf_clean_border, center_lat, center_lon


def visualize_with_folium(gdf_clean_border, center_lat, center_lon):
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles='cartodbpositron' 
    )

    folium.GeoJson(
        gdf_clean_border.to_json(),
        name='Clean Border',
        style_function=lambda x: {
            'fillColor': '#4285F4',
            'color': '#1A73E8', 
            'weight': 3,
            'fillOpacity': 0.3
        }
    ).add_to(m)
    
    map_file = create_output_path("ukraine_clean_border_folium_map.html")
    m.save(map_file)
    print(f"Folium map saved: {os.path.abspath(map_file)}")


def visualize_with_matplotlib(gdf_clean_border):
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    
    gdf_clean_border.plot(
        ax=ax, 
        edgecolor='#1A73E8', 
        facecolor='#AECBFA', 
        linewidth=1,
        alpha=0.7
    )
    
    ax.set_title("Clean Border")
    ax.set_aspect('equal')
    
    image_file = create_output_path("ukraine_clean_border_matplotlib.png")
    plt.savefig(image_file, dpi=300)
    plt.close(fig)
    print(f"MPL png saved: {os.path.abspath(image_file)}")


def visualize_clean_border():
    engine = setup_db_engine()
    gdf_clean_border, center_lat, center_lon = load_data(engine)

    visualize_with_folium(gdf_clean_border, center_lat, center_lon)
    visualize_with_matplotlib(gdf_clean_border)
    
    print("\nVisualization completed successfully.")


if __name__ == "__main__":
    visualize_clean_border()
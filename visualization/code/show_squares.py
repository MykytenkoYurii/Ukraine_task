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

from config import DB_CONFIG, TABLE_NAMES, GEOM_PARAMS

CLEAN_BORDER_TABLE = TABLE_NAMES['CLEAN_BORDER']
GRID_TABLE = TABLE_NAMES['GRID']
CENTER_TABLE = TABLE_NAMES['CENTER']
SQUARE_SIZE_KM = GEOM_PARAMS['SQUARE_SIZE_M'] / 1000

GEOM_COLUMN = 'geom'

OUTPUT_SUBDIR = PROJECT_ROOT.joinpath('visualization', 'output', 'squares')


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
    
    gdf_single_border = gpd.read_postgis(
        f"SELECT {GEOM_COLUMN} FROM {CLEAN_BORDER_TABLE}", 
        engine, 
        geom_col=GEOM_COLUMN
    )
    
    gdf_grid = gpd.read_postgis(
        f"SELECT * FROM {GRID_TABLE}", 
        engine, 
        geom_col='geom', 
        crs=3857
    ).to_crs(epsg=4326)

    df_center = pd.read_sql(f"SELECT center_lon, center_lat FROM {CENTER_TABLE}", engine)
    center_lat = df_center.iloc[0]['center_lat']
    center_lon = df_center.iloc[0]['center_lon']

    return gdf_single_border, gdf_grid, center_lat, center_lon


def visualize_with_folium(gdf_single_border, gdf_grid, center_lat, center_lon):
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles='cartodbpositron'
    )

    folium.GeoJson(
        gdf_grid.to_json(),
        name=f'Сітка {SQUARE_SIZE_KM}км',
        style_function=lambda x: {
            'fillColor': '#FFA07A', 'color': '#FF6347', 'weight': 1.5, 'fillOpacity': 0.3
        }
    ).add_to(m)

    folium.GeoJson(
        gdf_single_border.to_json(),
        name='Зовнішній Кордон України',
        style_function=lambda x: {
            'fillColor': 'blue', 'color': 'darkblue', 'weight': 3, 'fillOpacity': 0.0
        }
    ).add_to(m)
    
    folium.LayerControl().add_to(m)

    map_file = create_output_path("ukraine_grid_folium_map.html")
    m.save(map_file)
    print(f"Folium map saved: {os.path.abspath(map_file)}")


def visualize_with_matplotlib(gdf_single_border, gdf_grid):
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 12))
    
    gdf_grid.plot(
        ax=ax, 
        edgecolor='#FF6347', 
        facecolor='#FFA07A', 
        linewidth=0.5,
        alpha=0.3
    )
    
    gdf_single_border.plot(
        ax=ax, 
        edgecolor='darkblue', 
        facecolor='none',
        linewidth=2,
    )
    
    ax.set_title(f"Розбиття карти України на сітку {SQUARE_SIZE_KM}x{SQUARE_SIZE_KM} км (Matplotlib)")
    ax.set_aspect('equal')
    
    image_file = create_output_path("ukraine_grid_matplotlib.png")
    plt.savefig(image_file, dpi=300)
    plt.close(fig)
    print(f"MPL png saved: {os.path.abspath(image_file)}")


def visualize_ukraine_grid():
    engine = setup_db_engine()
    gdf_single_border, gdf_grid, center_lat, center_lon = load_data(engine)

    visualize_with_folium(gdf_single_border, gdf_grid, center_lat, center_lon)

    visualize_with_matplotlib(gdf_single_border, gdf_grid)
    
    print("\nVisualization completed successfully.")


if __name__ == "__main__":
    visualize_ukraine_grid()
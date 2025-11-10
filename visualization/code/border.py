import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from config import DB_CONFIG, TABLE_NAMES

FILE_PATH = PROJECT_ROOT / "dataset" / "ukraine_border.geojson" 
TABLE_NAME = TABLE_NAMES['BORDER']

def setup_db_engine():
    engine_string = (
        f"postgresql://{DB_CONFIG['USER']}:{DB_CONFIG['PASSWORD']}@"
        f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['NAME']}"
    )
    return create_engine(engine_string)

def import_border_data():
    engine = setup_db_engine()
    
    gdf = gpd.read_file(FILE_PATH)
    
    gdf.to_postgis(TABLE_NAME, engine, if_exists='replace', index=False)
    
if __name__ == "__main__":
    import_border_data()
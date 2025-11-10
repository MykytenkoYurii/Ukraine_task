DB_CONFIG = {
    'USER': 'postgres',  
    'PASSWORD': '123',  
    'HOST': 'localhost',
    'PORT': '5432',
    'NAME': 'Ukraine_border'  
}

TABLE_NAMES = {
    'BORDER': 'ukraine_border',              
    'CENTER': 'ukraine_center',              
    'RAW_UNION_SAFE': 'ukraine_raw_union_safe', 
    'CLEAN_BORDER': 'ukraine_clean_border',  
    'GRID': 'ukraine_grid',                  
    'VERTICES': 'grid_vertices',             
    'SECTORS': 'all_sectors'   
}              

GEOM_PARAMS = {
    'SQUARE_SIZE_M': 2000,
    'SECTOR_RADIUS_M': 5000,
    'CLEANUP_BUFFER_DEG': 0.001
}
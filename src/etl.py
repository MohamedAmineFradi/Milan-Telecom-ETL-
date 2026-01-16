import geopandas as gpd
import pandas as pd
import logging
from .config import DATA_DIR, MILANO_GRID_FILE, PROVINCES_FILE, TRAFFIC_PATTERN, MOBILITY_PATTERN, TARGET_CRS
from .database import get_sqlalchemy_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_grid_geometries():
    try:
        logger.info(f"Loading {MILANO_GRID_FILE}")
        
        engine = get_sqlalchemy_engine()
        existing_count = pd.read_sql("SELECT COUNT(*) FROM dim_grid_milan", engine).iloc[0, 0]
        
        if existing_count > 0:
            logger.info(f"✓ {existing_count} grid cells already loaded (skipping)")
            return
        
        gdf = gpd.read_file(MILANO_GRID_FILE)
        
        if gdf.crs != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
        
        gdf['cell_id'] = gdf.index
        
        gdf[['cell_id', 'geometry']].to_postgis(
            'dim_grid_milan',
            engine,
            if_exists='append',
            index=False
        )
        
        logger.info(f"✓ {len(gdf)} grid cells loaded")
        
    except Exception as e:
        logger.error(f"Grid loading error: {e}")
        raise


def load_provinces_geometries():
    try:
        logger.info(f"Loading {PROVINCES_FILE}")
        
        engine = get_sqlalchemy_engine()
        existing_count = pd.read_sql("SELECT COUNT(*) FROM dim_provinces_it", engine).iloc[0, 0]
        
        if existing_count > 0:
            logger.info(f"✓ {existing_count} provinces already loaded (skipping)")
            return
        
        gdf = gpd.read_file(PROVINCES_FILE)
        
        if gdf.crs != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
        
        if 'PROVINCIA' in gdf.columns:
            gdf = gdf.rename(columns={'PROVINCIA': 'provincia'})
        elif 'name' in gdf.columns:
            gdf = gdf.rename(columns={'name': 'provincia'})
        
        gdf[['provincia', 'geometry']].to_postgis(
            'dim_provinces_it',
            engine,
            if_exists='append',
            index=False
        )
        
        logger.info(f"✓ {len(gdf)} provinces loaded")
        
    except Exception as e:
        logger.error(f"Provinces loading error: {e}")
        raise


def load_traffic_data(file_pattern=None, limit_files=None):
    try:
        engine = get_sqlalchemy_engine()

        existing_count = pd.read_sql(
            "SELECT COUNT(*) as count FROM fact_traffic_milan",
            engine
        ).iloc[0]['count']

        if existing_count > 0:
            logger.info(f"✓ {existing_count} traffic rows already loaded (skipping)")
            return

        pattern = file_pattern or TRAFFIC_PATTERN
        csv_files = sorted(DATA_DIR.glob(pattern))
        
        if limit_files:
            csv_files = csv_files[:limit_files]
        
        if not csv_files:
            logger.warning(f"No files found for pattern: {pattern}")
            return
        
        logger.info(f"Loading {len(csv_files)} traffic files...")
        
        total_rows = 0
        
        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            df = df.rename(columns={'CellID': 'cell_id'})
            
            df = df[df['cell_id'].between(0, 9999)]
            
            df.to_sql(
                'fact_traffic_milan',
                engine,
                if_exists='append',
                index=False,
                chunksize=100
            )
            total_rows += len(df)
        
        logger.info(f"✓ {total_rows} traffic rows loaded from {len(csv_files)} files")
        
    except Exception as e:
        logger.error(f"Traffic data loading error: {e}")
        raise


def load_mobility_data(file_pattern=None, limit_files=None):
    try:
        pattern = file_pattern or MOBILITY_PATTERN
        csv_files = sorted(DATA_DIR.glob(pattern))
        
        if limit_files:
            csv_files = csv_files[:limit_files]
        
        if not csv_files:
            logger.warning(f"No files found for pattern: {pattern}")
            return
        
        logger.info(f"Loading {len(csv_files)} mobility files...")
        
        engine = get_sqlalchemy_engine()
        total_rows = 0
        
        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            df = df.rename(columns={
                'CellID': 'cell_id',
                'provinceName': 'provincia',
                'cell2Province': 'cell_to_province',
                'Province2cell': 'province_to_cell'
            })
            
            df = df[df['cell_id'].between(0, 9999)]
            
            df.to_sql(
                'fact_mobility_provinces',
                engine,
                if_exists='append',
                index=False,
                chunksize=100
            )
            total_rows += len(df)
        
        logger.info(f"✓ {total_rows} mobility rows loaded from {len(csv_files)} files")
        
    except Exception as e:
        logger.error(f"Mobility data loading error: {e}")
        raise


def get_top_cells(limit=10):
    query = f"""
    SELECT cell_id, AVG(total_activity) as avg_load 
    FROM v_hourly_traffic 
    WHERE hour >= '2013-11-01 00:00'::timestamptz
    GROUP BY cell_id 
    ORDER BY avg_load DESC 
    LIMIT {limit};
    """
    
    try:
        engine = get_sqlalchemy_engine()
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise

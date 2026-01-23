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
            logger.info(f"✓ {existing_count} grid cells already loaded (skipping new load)")
            # Backfill bounds if they are missing
            from sqlalchemy import text
            with engine.begin() as conn:
                conn.execute(text(
                    """
                    UPDATE dim_grid_milan
                    SET bounds = COALESCE(bounds, ST_AsText(ST_Envelope(geometry)))
                    WHERE bounds IS NULL
                    """
                ))
            return
        
        gdf = gpd.read_file(MILANO_GRID_FILE)
        
        if gdf.crs != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
        
        gdf['cell_id'] = gdf.index

        bounds_df = gdf.geometry.bounds
        gdf['bounds'] = bounds_df.apply(
            lambda row: f"{row.minx},{row.miny},{row.maxx},{row.maxy}", axis=1
        )
        
        gdf[['cell_id', 'geometry', 'bounds']].to_postgis(
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

        if 'population' in gdf.columns:
            gdf['population'] = pd.to_numeric(gdf['population'], errors='coerce').fillna(0).astype(int)
        else:
            gdf['population'] = 0
        
        gdf[['provincia', 'geometry', 'population']].to_postgis(
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
        rejected_rows = []
        
        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            initial_count = len(df)
            invalid_dates = 0
            invalid_cells = 0
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                before = len(df)
                df = df.dropna(subset=['datetime'])
                invalid_dates = before - len(df)
                if invalid_dates:
                    logger.warning(f"    - {invalid_dates} invalid dates will be dropped")
            
            df = df.rename(columns={'CellID': 'cell_id'})

            metric_cols = ['smsin', 'smsout', 'callin', 'callout', 'internet']
            for col in metric_cols:
                if col not in df.columns:
                    df[col] = 0
                else:
                    neg_count = (df[col] < 0).sum()
                    if neg_count > 0:
                        logger.warning(f"    - {neg_count} negative values in {col}, setting to 0")
            df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
            for col in metric_cols:
                df.loc[df[col] < 0, col] = 0

            invalid_cells = df[~df['cell_id'].between(0, 9999)].shape[0]
            df = df[df['cell_id'].between(0, 9999)]

            final_count = len(df)
            rejected = initial_count - final_count
            if rejected:
                logger.info(f"    - Cleaned: {rejected} rows rejected ({final_count} kept)")
                rejected_rows.append({
                    'file': csv_file.name,
                    'initial': initial_count,
                    'final': final_count,
                    'rejected': rejected,
                    'invalid_dates': invalid_dates,
                    'invalid_cells': invalid_cells
                })
            
            df.to_sql(
                'fact_traffic_milan',
                engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            total_rows += len(df)
        
        logger.info(f"✓ {total_rows} traffic rows loaded from {len(csv_files)} files")
        if rejected_rows:
            total_rejected = sum(r['rejected'] for r in rejected_rows)
            logger.info(f"⚠ {total_rejected} total rows were rejected during cleaning")
        
    except Exception as e:
        logger.error(f"Traffic data loading error: {e}")
        raise


def load_mobility_data(file_pattern=None, limit_files=None):
    try:
        engine = get_sqlalchemy_engine()

        existing_count = pd.read_sql(
            "SELECT COUNT(*) as count FROM fact_mobility_provinces",
            engine
        ).iloc[0]['count']

        if existing_count > 0:
            logger.info(f"✓ {existing_count} mobility rows already loaded (skipping)")
            return

        pattern = file_pattern or MOBILITY_PATTERN
        csv_files = sorted(DATA_DIR.glob(pattern))
        
        if limit_files:
            csv_files = csv_files[:limit_files]
        
        if not csv_files:
            logger.warning(f"No files found for pattern: {pattern}")
            return
        
        logger.info(f"Loading {len(csv_files)} mobility files...")
        
        total_rows = 0
        
        province_map = {
            "Monza E Della Brianza": "Monza e della Brianza",
            "Reggio Nell'Emilia": "Reggio nell'Emilia",
            "Reggio Di Calabria": "Reggio di Calabria",
            "Pesaro E Urbino": "Pesaro e Urbino",
            "Massa-Carrara": "Massa Carrara",
            "Valle D'Aosta": "Aosta",
            "Bolzano/Bozen": "Bolzano",
        }

        valid_provinces = pd.read_sql(
            "SELECT provincia FROM dim_provinces_it",
            engine
        )['provincia']

        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                before = len(df)
                df = df.dropna(subset=['datetime'])
                dropped_null = before - len(df)
                if dropped_null:
                    logger.info(f"    - dropped {dropped_null} rows with null/invalid datetime from {csv_file.name}")
            
            df = df.rename(columns={
                'CellID': 'cell_id',
                'provinceName': 'provincia',
                'cell2Province': 'cell2province',
                'Province2cell': 'province2cell'
            })

            for col in ['cell2province', 'province2cell']:
                if col not in df.columns:
                    df[col] = 0
            df[['cell2province', 'province2cell']] = df[['cell2province', 'province2cell']].apply(pd.to_numeric, errors='coerce').fillna(0)

            if 'provincia' in df.columns:
                df['provincia'] = df['provincia'].str.title().str.strip()
                df['provincia'] = df['provincia'].replace(province_map)
                before = len(df)
                df = df[df['provincia'].isin(valid_provinces)]
                dropped = before - len(df)
                if dropped:
                    logger.info(f"    - dropped {dropped} rows with unmatched provinces from {csv_file.name}")
            
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


def validate_schema_constraints(engine=None):
    """Check simple non-negative constraints at the DB level."""
    engine = engine or get_sqlalchemy_engine()
    constraints = [
        ("dim_grid_milan", "(cell_id BETWEEN 0 AND 9999)"),
        ("dim_provinces_it", "(population >= 0)"),
        ("fact_traffic_milan", "(smsin >= 0)"),
        ("fact_traffic_milan", "(smsout >= 0)"),
        ("fact_traffic_milan", "(callin >= 0)"),
        ("fact_traffic_milan", "(callout >= 0)"),
        ("fact_traffic_milan", "(internet >= 0)"),
        ("fact_mobility_provinces", "(cell2province >= 0)"),
        ("fact_mobility_provinces", "(province2cell >= 0)")
    ]

    for table, condition in constraints:
        query = f"""
        SELECT COUNT(*) AS violations
        FROM {table}
        WHERE NOT {condition}
        """
        df = pd.read_sql(query, engine)
        violations = int(df.iloc[0]['violations'])
        if violations > 0:
            logger.warning(f"⚠ {violations} violations in {table} for constraint {condition}")
        else:
            logger.info(f"✓ {table}: {condition} - No violations")

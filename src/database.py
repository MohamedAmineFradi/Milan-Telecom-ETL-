import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import logging
from .config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_sqlalchemy_engine():
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(connection_string)


def create_database():
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['dbname']}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['dbname']}")
            logger.info(f"Database '{DB_CONFIG['dbname']}' created")
        else:
            logger.info(f"Database '{DB_CONFIG['dbname']}' already exists")
        
        cursor.close()
        conn.close()
        
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        conn.commit()
        logger.info("PostGIS extension enabled")
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database creation error: {e}")
        raise


def create_schema():
    schema_sql = """
    CREATE TABLE IF NOT EXISTS dim_grid_milan (
        cell_id INTEGER PRIMARY KEY,
        geometry GEOMETRY(POLYGON, 32632),
        bounds TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dim_provinces_it (
        provincia VARCHAR(50) PRIMARY KEY,
        geometry GEOMETRY(MULTIPOLYGON, 32632),
        population INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS fact_traffic_milan (
        datetime TIMESTAMPTZ,
        cell_id INTEGER REFERENCES dim_grid_milan(cell_id),
        countrycode INTEGER,
        smsin NUMERIC DEFAULT 0 NOT NULL, 
        smsout NUMERIC DEFAULT 0 NOT NULL,
        callin NUMERIC DEFAULT 0 NOT NULL, 
        callout NUMERIC DEFAULT 0 NOT NULL,
        internet NUMERIC DEFAULT 0 NOT NULL,
        PRIMARY KEY (datetime, cell_id, countrycode)
    );

    CREATE TABLE IF NOT EXISTS fact_mobility_provinces (
        datetime TIMESTAMPTZ,
        cell_id INTEGER REFERENCES dim_grid_milan(cell_id),
        provincia VARCHAR(50) REFERENCES dim_provinces_it(provincia),
        cell2province NUMERIC DEFAULT 0 NOT NULL,
        province2cell NUMERIC DEFAULT 0 NOT NULL
    );

    -- Enforce defaults and clean up existing nulls
    ALTER TABLE dim_provinces_it
        ALTER COLUMN population SET DEFAULT 0;
    UPDATE dim_provinces_it SET population = 0 WHERE population IS NULL;

    UPDATE dim_grid_milan
    SET bounds = COALESCE(bounds, ST_AsText(ST_Envelope(geometry)))
    WHERE bounds IS NULL;

    ALTER TABLE fact_traffic_milan
        ALTER COLUMN smsin SET DEFAULT 0,
        ALTER COLUMN smsout SET DEFAULT 0,
        ALTER COLUMN callin SET DEFAULT 0,
        ALTER COLUMN callout SET DEFAULT 0,
        ALTER COLUMN internet SET DEFAULT 0;
    UPDATE fact_traffic_milan
    SET
        smsin = COALESCE(smsin, 0),
        smsout = COALESCE(smsout, 0),
        callin = COALESCE(callin, 0),
        callout = COALESCE(callout, 0),
        internet = COALESCE(internet, 0);
    DELETE FROM fact_traffic_milan WHERE datetime IS NULL;

    ALTER TABLE fact_mobility_provinces
        ALTER COLUMN cell2province SET DEFAULT 0,
        ALTER COLUMN province2cell SET DEFAULT 0;
    UPDATE fact_mobility_provinces
    SET
        cell2province = COALESCE(cell2province, 0),
        province2cell = COALESCE(province2cell, 0);
    DELETE FROM fact_mobility_provinces WHERE datetime IS NULL;

    CREATE OR REPLACE VIEW v_hourly_traffic AS
    SELECT DATE_TRUNC('hour', datetime) AS hour,
           cell_id,
           SUM(smsin+smsout+callin+callout+internet) AS total_activity
    FROM fact_traffic_milan 
    GROUP BY 1, 2;

    CREATE INDEX IF NOT EXISTS idx_grid_geom ON dim_grid_milan USING GIST(geometry);
    CREATE INDEX IF NOT EXISTS idx_traffic_time ON fact_traffic_milan(datetime);
    """
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(schema_sql)
        conn.commit()
        logger.info("Schema created successfully")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Schema creation error: {e}")
        raise


def execute_query(query, fetch=True):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        if fetch:
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results
        else:
            conn.commit()
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise

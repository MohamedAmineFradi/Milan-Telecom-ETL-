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


def create_schema(drop_existing: bool = False):
    drop_sql = "" if not drop_existing else """
    DROP TABLE IF EXISTS fact_mobility_provinces CASCADE;
    DROP TABLE IF EXISTS fact_traffic_milan CASCADE;
    DROP TABLE IF EXISTS dim_provinces_it CASCADE;
    DROP TABLE IF EXISTS dim_grid_milan CASCADE;
    """

    schema_sql = f"""
    {drop_sql}
    CREATE TABLE IF NOT EXISTS dim_grid_milan (
        cell_id INTEGER PRIMARY KEY CHECK (cell_id BETWEEN 0 AND 9999),
        geometry GEOMETRY(POLYGON, 32632) NOT NULL,
        bounds TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dim_provinces_it (
        provincia VARCHAR(50) PRIMARY KEY,
        geometry GEOMETRY(MULTIPOLYGON, 32632) NOT NULL,
        population INTEGER DEFAULT 0 CHECK (population >= 0)
    );

    CREATE TABLE IF NOT EXISTS fact_traffic_milan (
        datetime TIMESTAMPTZ NOT NULL,
        cell_id INTEGER NOT NULL REFERENCES dim_grid_milan(cell_id),
        countrycode INTEGER NOT NULL,
        smsin NUMERIC DEFAULT 0 NOT NULL CHECK (smsin >= 0), 
        smsout NUMERIC DEFAULT 0 NOT NULL CHECK (smsout >= 0),
        callin NUMERIC DEFAULT 0 NOT NULL CHECK (callin >= 0), 
        callout NUMERIC DEFAULT 0 NOT NULL CHECK (callout >= 0),
        internet NUMERIC DEFAULT 0 NOT NULL CHECK (internet >= 0),
        PRIMARY KEY (datetime, cell_id, countrycode)
    );

    CREATE TABLE IF NOT EXISTS fact_mobility_provinces (
        datetime TIMESTAMPTZ NOT NULL,
        cell_id INTEGER NOT NULL REFERENCES dim_grid_milan(cell_id),
        provincia VARCHAR(50) NOT NULL REFERENCES dim_provinces_it(provincia),
        cell2province NUMERIC DEFAULT 0 NOT NULL CHECK (cell2province >= 0),
        province2cell NUMERIC DEFAULT 0 NOT NULL CHECK (province2cell >= 0)
    );

    CREATE OR REPLACE VIEW v_hourly_traffic AS
    SELECT 
        DATE_TRUNC('hour', datetime) AS hour,
        cell_id,
        SUM(smsin) AS total_smsin,
        SUM(smsout) AS total_smsout,
        SUM(callin) AS total_callin,
        SUM(callout) AS total_callout,
        SUM(internet) AS total_internet,
        SUM(smsin + smsout + callin + callout + internet) AS total_activity
    FROM fact_traffic_milan 
    GROUP BY 1, 2;

    CREATE INDEX IF NOT EXISTS idx_grid_geom ON dim_grid_milan USING GIST(geometry);
    CREATE INDEX IF NOT EXISTS idx_traffic_time ON fact_traffic_milan(datetime);
    CREATE INDEX IF NOT EXISTS idx_traffic_cell ON fact_traffic_milan(cell_id);
    CREATE INDEX IF NOT EXISTS idx_traffic_composite ON fact_traffic_milan(cell_id, datetime);
    CREATE INDEX IF NOT EXISTS idx_mobility_provincia ON fact_mobility_provinces(provincia);
    CREATE INDEX IF NOT EXISTS idx_mobility_cell ON fact_mobility_provinces(cell_id);
    CREATE INDEX IF NOT EXISTS idx_mobility_datetime ON fact_mobility_provinces(datetime);
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

"""
Database Connection untuk PostgreSQL
"""
import psycopg2
from psycopg2.extras import execute_batch
import logging

logger = logging.getLogger(__name__)

# KONFIGURASI DATABASE 
# menggunakan 'host.docker.internal' karena Airflow berjalan di Docker
DB_CONFIG = {
    'host': 'host.docker.internal',  
    'port': 5432,
    'database': 'postgres',          
    'user': 'postgres',
    'password': ''                   
}

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Berhasil terkoneksi ke PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Gagal koneksi database: {e}")
        raise

# Insert data 
def insert_batch(query, data_list):
    conn = None
    try:
        # Buat koneksi
        conn = get_connection()
        cursor = conn.cursor()
        
        # Insert batch
        execute_batch(cursor, query, data_list, page_size=1000)
        
        # Commit
        conn.commit()
        logger.info(f"Berhasil insert {len(data_list)} records")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error insert batch: {e}")
        raise
    finally:
        if conn:
            conn.close()

#Ambil data dari database
def fetch_all(query, params=None):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    
    except Exception as e:
        logger.error(f"Error fetch data: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Eksekusi query tanpa hasil (misal: CREATE, TRUNCATE)
def execute_query(query):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logger.info(f"Query executed: {query[:50]}")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error execute query: {e}")
        raise

    finally:
        if conn:
            conn.close()

import pandas as pd
import logging
import os
from utils.db_connection import insert_batch, fetch_all, execute_query

logger = logging.getLogger(__name__)

# Dataset silver files
SILVER_FILES = [
    'permohonan_kp_silver.csv', 
    'permohonan_ta_silver.csv',
    'pendaftaran_semester_antara_silver.csv', 
    'mbkm_mandiri_silver.csv',
    'tidak_alih_jenjang_silver.csv', 
    'keterangan_mahasiswa_aktif_silver.csv',
    'besaran_ukt_silver.csv',
    'pengunduran_diri_silver.csv',
    'pengajuan_cuti_silver.csv',
    'rekomendasi_beasiswa_lomba_silver.csv'
]


def init_schema():
    try:
        schema_file = os.path.join(os.path.dirname(__file__), '../config/schema_datamart.sql')
        
        if not os.path.exists(schema_file):
            logger.error(f"Schema file not found: {schema_file}")
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        with open(schema_file, 'r') as f:
            sql_script = f.read()
        
        from utils.db_connection import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("DROP SCHEMA IF EXISTS datamart CASCADE")
            conn.commit()
            
            statements = sql_script.split(';')
            for statement in statements:
                lines = [l.strip() for l in statement.split('\n') if l.strip() and not l.strip().startswith('--')]
                clean_statement = '\n'.join(lines)
                
                if clean_statement:
                    cursor.execute(clean_statement)
            
            conn.commit()
            logger.info("Schema initialized successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error initializing schema: {e}")
            raise
        finally:
            conn.close()
                
    except Exception as e:
        logger.error(f"Fatal error in init_schema(): {e}")
        raise

# DIMENSION TABLES LOADING
#Load dimension table: status layanan (Pending, Sudah Dilayani)
def load_dim_status():
    data = [('Pending',), ('Sudah Dilayani',)]
    query = "INSERT INTO datamart.dim_status_layanan (status_layanan) VALUES (%s) ON CONFLICT ON CONSTRAINT dim_status_layanan_status_layanan_key DO NOTHING"
    insert_batch(query, data)
    logger.info(f"Status: {len(data)} records loaded")

#Load dimension table: unique types of services (jenis_layanan, jenis_permohonan, dll.).
def load_dim_jenis_layanan(silver_path):
    all_jenis = set()
    
    for file in SILVER_FILES:
        try:
            df = pd.read_csv(f"{silver_path}/{file}")
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            for col in df.columns:
                if 'jenis' in col:
                    values = df[col].dropna().astype(str).str.strip()
                    values = values[values != ''].unique()
                    all_jenis.update(values)
        except Exception:
            pass
    
    data = [(jenis,) for jenis in sorted(all_jenis) if jenis and jenis != 'nan']
    query = "INSERT INTO datamart.dim_layanan_jenis (nama_layanan) VALUES (%s) ON CONFLICT ON CONSTRAINT dim_layanan_jenis_nama_layanan_key DO NOTHING"
    insert_batch(query, data)
    logger.info(f"Jenis Layanan: {len(data)} records loaded")

#Load dimension table: unique students (NIM as primary key)
def load_dim_mahasiswa(silver_path):
    all_mahasiswa = []
    
    for file in SILVER_FILES:
        try:
            df = pd.read_csv(f"{silver_path}/{file}")
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            nim_col = None
            nama_col = None
            program_col = None
            
            for col in df.columns:
                if 'nim' in col and nim_col is None:
                    nim_col = col
                elif 'nama' in col and nama_col is None:
                    nama_col = col
                elif ('program' in col or 'prodi' in col) and program_col is None:
                    program_col = col
            
            if not nim_col or not nama_col:
                continue
            
            cols_to_select = [c for c in [nim_col, nama_col, program_col] if c and c in df.columns]
            if not cols_to_select:
                continue
                
            df_temp = df[cols_to_select].copy()
            
            rename_map = {}
            if nim_col in df_temp.columns:
                rename_map[nim_col] = 'nim'
            if nama_col in df_temp.columns:
                rename_map[nama_col] = 'nama_mahasiswa'
            if program_col in df_temp.columns:
                rename_map[program_col] = 'program_studi'
            
            df_temp = df_temp.rename(columns=rename_map)
            
            if 'program_studi' not in df_temp.columns:
                df_temp['program_studi'] = None
            
            df_temp = df_temp.dropna(subset=['nim', 'nama_mahasiswa'])
            df_temp['nim'] = df_temp['nim'].astype(str).str.strip()
            df_temp['nama_mahasiswa'] = df_temp['nama_mahasiswa'].astype(str).str.strip()
            df_temp = df_temp[(df_temp['nim'] != '') & (df_temp['nama_mahasiswa'] != '')]
            
            df_temp['program_studi'] = df_temp['program_studi'].fillna('').astype(str).str.strip()
            
            all_mahasiswa.append(df_temp)
        except Exception:
            pass
    
    if not all_mahasiswa:
        logger.warning("No mahasiswa data found")
        return
    
    df_all = pd.concat(all_mahasiswa, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=['nim'])
    
    if len(df_all) == 0:
        logger.warning("No valid mahasiswa records after deduplication")
        return
    
    data = list(df_all[['nim', 'nama_mahasiswa', 'program_studi']].itertuples(index=False, name=None))
    query = """
        INSERT INTO datamart.dim_mahasiswa (nim, nama_mahasiswa, program_studi) 
        VALUES (%s, %s, %s)
        ON CONFLICT (nim) DO UPDATE SET
            nama_mahasiswa = EXCLUDED.nama_mahasiswa,
            program_studi = EXCLUDED.program_studi
    """
    insert_batch(query, data)
    logger.info(f"Mahasiswa: {len(data)} records loaded")


#Load dimension table: unique timestamps with date-time components
def load_dim_waktu(silver_path):
    all_timestamps = []
    
    for file in SILVER_FILES:
        try:
            df = pd.read_csv(f"{silver_path}/{file}")
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            if 'timestamp' in df.columns:
                all_timestamps.extend(df['timestamp'].dropna().tolist())
        except Exception:
            pass
    
    df_waktu = pd.DataFrame({'timestamp': all_timestamps})
    df_waktu['timestamp'] = pd.to_datetime(df_waktu['timestamp'], dayfirst=True, format='mixed')
    df_waktu['tanggal'] = df_waktu['timestamp'].dt.date
    df_waktu['jam'] = df_waktu['timestamp'].dt.time
    df_waktu['hour'] = df_waktu['timestamp'].dt.hour
    df_waktu['hari'] = df_waktu['timestamp'].dt.day_name()
    df_waktu['bulan'] = df_waktu['timestamp'].dt.month_name()
    df_waktu['tahun'] = df_waktu['timestamp'].dt.year
    
    df_waktu = df_waktu.drop_duplicates(subset=['tanggal', 'jam'])
    data = list(df_waktu[['tanggal', 'jam', 'hour', 'hari', 'bulan', 'tahun']].itertuples(index=False, name=None))
    
    query = """
        INSERT INTO datamart.dim_waktu (tanggal, jam, hour, hari, bulan, tahun) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON CONFLICT (tanggal, jam) DO NOTHING
    """
    insert_batch(query, data)
    logger.info(f"Waktu: {len(data)} records loaded")


# HELPER FUNCTIONS
#Retrieve time dimension ID from timestamp
def get_waktu_id(timestamp):
    try:
        dt = pd.to_datetime(timestamp)
        result = fetch_all(
            "SELECT id_waktu FROM datamart.dim_waktu WHERE tanggal = %s AND jam = %s LIMIT 1",
            (dt.date(), dt.time())  
        )
        return result[0][0] if result else None
    except:
        return None



# FACT TABLE LOADING
#Load fact table: service transactions for students
def load_fact_layanan(silver_path):
    execute_query("TRUNCATE TABLE datamart.fact_layanan_mahasiswa")
    
    status_map = {row[1]: row[0] for row in fetch_all("SELECT id_status, status_layanan FROM datamart.dim_status_layanan")}
    layanan_map = {row[1]: row[0] for row in fetch_all("SELECT id_layanan_jenis, nama_layanan FROM datamart.dim_layanan_jenis")}
    mahasiswa_data = fetch_all("SELECT nim FROM datamart.dim_mahasiswa")
    mahasiswa_nims = {str(row[0]).strip(): str(row[0]).strip() for row in mahasiswa_data}
    
    all_facts = []
    failed_records = 0
    
    for file in SILVER_FILES:
        try:
            df = pd.read_csv(f"{silver_path}/{file}")
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            for idx, row in df.iterrows():
                try:
                    nim = ''
                    status = ''
                    jenis = ''
                    
                    for col in df.columns:
                        if 'nim' in col and not nim:
                            nim = str(row.get(col, '')).strip()
                        elif col == 'status':
                            status = str(row.get(col, '')).strip()
                        elif 'jenis' in col and not jenis:
                            jenis = str(row.get(col, '')).strip()
                        elif col == 'timestamp' and 'timestamp' not in locals():
                            timestamp = row.get(col, '')
                    
                    nim_valid = mahasiswa_nims.get(nim)
                    id_status = status_map.get(status)
                    id_jenis = layanan_map.get(jenis)
                    id_waktu = get_waktu_id(str(row.get('timestamp', '')))
                    
                    if all([nim_valid, id_status, id_jenis, id_waktu]):
                        all_facts.append((nim_valid, id_status, id_jenis, id_waktu))
                    else:
                        failed_records += 1
                        
                except Exception:
                    failed_records += 1
                    continue
                    
        except Exception:
            pass
    
    if all_facts:
        query = """
            INSERT INTO datamart.fact_layanan_mahasiswa 
            (nim, id_status, id_layanan_jenis, id_waktu) 
            VALUES (%s, %s, %s, %s)
        """
        insert_batch(query, all_facts)
    
    logger.info(f"Fact Layanan: {len(all_facts)} records inserted")



# MAIN FUNCTION
def load_all_to_gold(silver_path):
    """
    Load all data to Gold Layer.
    1. Initialize schema and tables
    2. Load dimension tables
    3. Load fact table 
    """
    try:
        init_schema()
        
        logger.info("\nLoading dimension tables...")
        load_dim_status()
        load_dim_jenis_layanan(silver_path)
        load_dim_mahasiswa(silver_path)
        load_dim_waktu(silver_path)
        logger.info("Dimension tables loaded successfully")
        
        logger.info("\nLoading fact table...")
        load_fact_layanan(silver_path)
        logger.info("Fact table loaded successfully")
        
        logger.info("="*70)
        logger.info("GOLD LOAD COMPLETE - Data loaded to PostgreSQL")
        logger.info("="*70)
        
    except Exception as e:
        logger.error("="*70)
        logger.error(f"GOLD LOAD FAILED: {e}")
        logger.error("="*70)
        raise


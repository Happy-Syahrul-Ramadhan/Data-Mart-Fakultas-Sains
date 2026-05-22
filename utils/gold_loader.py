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

#Load dimension table: unique students (surrogate key id_mahasiswa)
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


# FACT TABLE LOADING
#Load fact table: service transactions for students
def load_fact_layanan(silver_path):
    execute_query("TRUNCATE TABLE datamart.fact_layanan_mahasiswa RESTART IDENTITY")

    def get_first_value(row, columns, keywords):
        for col in columns:
            if any(keyword in col for keyword in keywords):
                value = row.get(col, '')
                if pd.notna(value) and str(value).strip() != '':
                    return str(value).strip()
        return ''

    source_rows = []

    for file in SILVER_FILES:
        try:
            df = pd.read_csv(f"{silver_path}/{file}")
            df.columns = df.columns.str.lower().str.replace(' ', '_')

            for _, row in df.iterrows():
                source_rows.append({
                    'nim': get_first_value(row, df.columns, ['nim']),
                    'nama_mahasiswa': get_first_value(row, df.columns, ['nama']),
                    'program_studi': get_first_value(row, df.columns, ['program', 'prodi']),
                    'status_layanan': get_first_value(row, df.columns, ['status']),
                    'nama_layanan': get_first_value(row, df.columns, ['jenis']),
                    'timestamp': get_first_value(row, df.columns, ['timestamp'])
                })
        except Exception as e:
            logger.warning(f"Skip file {file}: {e}")

    if not source_rows:
        logger.warning("No fact source rows found")
        return

    fact_df = pd.DataFrame(source_rows)
    fact_df = fact_df[fact_df['nim'] != '']
    fact_df = fact_df[fact_df['status_layanan'] != '']
    fact_df = fact_df[fact_df['nama_layanan'] != '']
    fact_df = fact_df[fact_df['timestamp'] != '']

    if fact_df.empty:
        logger.warning("No valid fact rows after source cleanup")
        return

    fact_df['timestamp'] = pd.to_datetime(fact_df['timestamp'], dayfirst=True, format='mixed', errors='coerce')
    fact_df = fact_df.dropna(subset=['timestamp'])
    fact_df['tanggal'] = fact_df['timestamp'].dt.date
    fact_df['jam'] = fact_df['timestamp'].dt.time

    mahasiswa_df = pd.DataFrame(
        fetch_all("SELECT id_mahasiswa, nim FROM datamart.dim_mahasiswa"),
        columns=['id_mahasiswa', 'nim']
    )

    fact_df = fact_df.merge(mahasiswa_df, on='nim', how='inner')
    jenis_df = pd.DataFrame(
        fetch_all("SELECT id_layanan_jenis, nama_layanan FROM datamart.dim_layanan_jenis"),
        columns=['id_layanan_jenis', 'nama_layanan']
    )
    status_df = pd.DataFrame(
        fetch_all("SELECT id_status, status_layanan FROM datamart.dim_status_layanan"),
        columns=['id_status', 'status_layanan']
    )
    waktu_df = pd.DataFrame(
        fetch_all("SELECT id_waktu, tanggal, jam, hari, bulan, tahun, hour FROM datamart.dim_waktu"),
        columns=['id_waktu', 'tanggal', 'jam', 'hari', 'bulan', 'tahun', 'hour']
    )

    fact_df = fact_df.merge(jenis_df, on='nama_layanan', how='inner')
    fact_df = fact_df.merge(status_df, on='status_layanan', how='inner')
    fact_df = fact_df.merge(waktu_df, on=['tanggal', 'jam'], how='inner')

    if fact_df.empty:
        logger.warning("No rows matched dimension joins for fact load")
        return

    fact_df['total_layanan_masuk'] = 1
    fact_df['total_layanan_pending'] = fact_df['status_layanan'].apply(lambda value: 1 if value == 'Pending' else 0)
    fact_df['total_layanan_sudah_dilayani'] = fact_df['status_layanan'].apply(lambda value: 1 if value == 'Sudah Dilayani' else 0)

    fact_df = fact_df.drop_duplicates(subset=['nim', 'status_layanan', 'nama_layanan', 'tanggal', 'jam'])

    fact_rows = list(
        fact_df[[
            'id_mahasiswa', 'nim', 'nama_mahasiswa', 'program_studi',
            'id_status', 'status_layanan',
            'id_layanan_jenis', 'nama_layanan',
            'id_waktu', 'tanggal', 'jam', 'hari', 'bulan', 'tahun', 'hour',
            'total_layanan_masuk', 'total_layanan_pending', 'total_layanan_sudah_dilayani'
        ]].itertuples(index=False, name=None)
    )

    query = """
        INSERT INTO datamart.fact_layanan_mahasiswa (
            id_mahasiswa, nim, nama_mahasiswa, program_studi,
            id_status, status_layanan,
            id_layanan_jenis, nama_layanan,
            id_waktu, tanggal, jam, hari, bulan, tahun, hour,
            total_layanan_masuk, total_layanan_pending, total_layanan_sudah_dilayani
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_batch(query, fact_rows)

    logger.info(f"Fact Layanan: {len(fact_rows)} records inserted")



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


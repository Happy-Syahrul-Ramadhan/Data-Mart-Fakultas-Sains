from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import sys
import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Import module data quality
sys.path.append('/opt/airflow')
from utils.data_quality_rules import apply_quality_checks, get_quality_rules_config

logger = logging.getLogger(__name__)

# KONFIGURASI
CREDENTIALS_PATH = '/opt/airflow/credentials/credentials.json'

# Konfigurasi Multiple Spreadsheets
SPREADSHEET_CONFIGS = [
    {
        'name': 'permohonan_kp',
        'spreadsheet_key': '1cjeJUxURVxmaE68mYa6Kl7ytAZ0HmhAwkcKrXkXtOgs',
        'sheet_name': 'permohonan_kp',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'Asal Program Studi',
            'Jenis Permohonan',
            'Nama Mahasiswa',
            'Nim Mahasiswa'
        ],
        'output_path': '/opt/airflow/data/bronze/permohonan_kp_bronze.csv'
    },
    {
        'name': 'permohonan_ta',
        'spreadsheet_key': '1fR9a_w9H8orZzWsjn5DvrJ1QJ8hxIg7IUJkNrZmSQ7s',
        'sheet_name': 'permohonan_ta',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'Jenis Permohonan',
            'Nama Mahasiswa',
            'Program Studi Mahasiswa',
            'NIM Mahasiswa'
        ],
        'output_path': '/opt/airflow/data/bronze/permohonan_ta_bronze.csv'
    },
    {
        'name': 'pendaftaran_semester_antara',
        'spreadsheet_key': '15-rDFUfe3DUlvvAhtP5_jBOVa0BoUZQAeaNSR-Gss28',
        'sheet_name': 'pendaftaran_semester_antara',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'jenis layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/pendaftaran_semester_antara_bronze.csv'
    },
    {
        'name': 'mbkm_mandiri',
        'spreadsheet_key': '1YwoaHAgvLCoCo0ARCfeLN4DS_HRROhYmV2FnvxKucDs',
        'sheet_name': 'mbkm_mandiri',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'jenis layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/mbkm_mandiri_bronze.csv'
    },
    {
        'name': 'tidak_alih_jenjang',
        'spreadsheet_key': '1g08OKUAwQp4F1cthOaoc8LdIxqmDsi5M-vmzGdtyFFo',
        'sheet_name': 'tidak_alih_jenjang',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'Jenis Layanan',
            'Nama',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/tidak_alih_jenjang_bronze.csv'
    },
    {
        'name': 'keterangan_mahasiswa_aktif',
        'spreadsheet_key': '1_f_mzM-Q6ZU1Ew07dp41rkw4XZ7XIEB_ksjRVXzXRT8',
        'sheet_name': 'keterangan_mahasiswa_aktif',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'jenis layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/keterangan_mahasiswa_aktif_bronze.csv'
    },
    {
        'name': 'besaran_ukt',
        'spreadsheet_key': '1rk69Lj0TtJMgjOxrtwMR8xoH-ah2OuAlJNgDVGPK154',
        'sheet_name': 'besaran_ukt',
        'selected_columns': [
            'id',
            'status',
            'Timestamp',
            'jenis layanan',
            'Nama',
            'Prodi',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/besaran_ukt_bronze.csv'
    },
    {
        'name': 'pengunduran_diri',
        'spreadsheet_key': '1-Bn8u-vXiCg6Z3haXcsLkVLG959KUXqb8KStgzTfhL0',
        'sheet_name': 'pengunduran_diri',
        'selected_columns': [
            'id',
            'Status',
            'Timestamp',
            'Jenis Layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM'
        ],
        'output_path': '/opt/airflow/data/bronze/pengunduran_diri_bronze.csv'
    },
    {
        'name': 'pengajuan_cuti',
        'spreadsheet_key': '1-VrAWo9qDK6qOU3ABbPpNrtvI4SyayOGYV2eJT055ko',
        'sheet_name': 'pengajuan_cuti',
        'selected_columns': [
            'id',
            'Status',
            'Timestamp',
            'Jenis Layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM Mahasiswa'
        ],
        'output_path': '/opt/airflow/data/bronze/pengajuan_cuti_bronze.csv'
    },
    {
        'name': 'rekomendasi_beasiswa_lomba',
        'spreadsheet_key': '1rOfR6ofBv1idz0EgV7L4-3k-JvHhJqWszN_i0hidmGM',
        'sheet_name': 'rekomendasi_beasiswa_lomba',
        'selected_columns': [
            'id',
            'Status',
            'Timestamp',
            'Jenis Layanan',
            'Nama Mahasiswa',
            'Program Studi',
            'NIM Mahasiswa'
        ],
        'output_path': '/opt/airflow/data/bronze/rekomendasi_beasiswa_lomba_bronze.csv'
    }
]

BRONZE_DIR = '/opt/airflow/data/bronze/'
SILVER_DIR = '/opt/airflow/data/silver/'
QUARANTINE_DIR = '/opt/airflow/data/quarantine/'

# FUNGSI EKSTRAKSI GOOGLE SHEETS
def koneksi_google_sheets(credentials_path):
    """Buat koneksi ke Google Sheets API"""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_path, 
        scope
    )
    
    client = gspread.authorize(credentials)
    logger.info("Berhasil terkoneksi ke Google Sheets API")
    return client


def ambil_data_dari_sheet(client, spreadsheet_key, sheet_name, selected_columns):
    """Ambil data dari Google Sheets dan convert ke DataFrame"""
    # Buka spreadsheet dan worksheet
    spreadsheet = client.open_by_key(spreadsheet_key)
    worksheet = spreadsheet.worksheet(sheet_name)
    
    # Ambil semua data
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)
    
    logger.info(f"Data diekstrak: {len(df)} baris")
    
    # Ambil hanya kolom yang dipilih
    if selected_columns:
        df = df[selected_columns]
    
    return df


def simpan_ke_csv(df, output_path):
    """Simpan DataFrame ke file CSV"""
    df.to_csv(output_path, index=False)
    logger.info(f"Disimpan ke: {output_path}")
    return output_path

# DEFAULT ARGUMENTS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# TASK FUNCTIONS 
# Fungsi untuk ekstraksi data dari Multiple Google Sheets ke Bronze layer
def task_extract_to_bronze(**context):
    
    # Koneksi ke Google Sheets
    try:
        client = koneksi_google_sheets(CREDENTIALS_PATH)
        logger.info("Terkoneksi ke Google Sheets")
    except Exception as e:
        logger.error(f"Gagal koneksi Google Sheets: {str(e)}")
        raise
    
    output_files = []
    success_count = 0
    fail_count = 0
    
    for config in SPREADSHEET_CONFIGS:
        try:            
            # Ambil data dari Google Sheets
            df = ambil_data_dari_sheet(
                client=client,
                spreadsheet_key=config['spreadsheet_key'],
                sheet_name=config['sheet_name'],
                selected_columns=config['selected_columns']
            )
            
            logger.info(f"Data: {len(df)} baris, {len(df.columns)} kolom")
            
            # Simpan ke CSV
            output_file = simpan_ke_csv(df, config['output_path'])
            output_files.append(output_file)
            success_count += 1
            logger.info(f"Berhasil: {output_file}")
            
        except Exception as e:
            fail_count += 1
            logger.error(f"Gagal: {str(e)}")
            continue
    
    logger.info("\n" + "="*60)
    logger.info(f"BRONZE COMPLETE: {success_count} success, {fail_count} failed")
    
    context['task_instance'].xcom_push(key='bronze_files', value=output_files)
    return output_files

# Task dari bronze ke silver 
def task_bronze_to_silver(**context):
    
    quality_rules_config = get_quality_rules_config()
    total_records = 0
    total_silver = 0
    total_quarantine = 0
    silver_files = []
    quarantine_files = []
    
    for config in SPREADSHEET_CONFIGS:
        dataset_name = config['name']
        bronze_file = config['output_path']
        
        try:
            if not os.path.exists(bronze_file):
                logger.warning(f"Bronze file tidak ditemukan: {bronze_file}")
                continue
            
            logger.info(f"\nTransform: {dataset_name}")
            
            df_bronze = pd.read_csv(bronze_file)
            records_in = len(df_bronze)
            total_records += records_in
            
            if records_in == 0:
                logger.warning(f"Tidak ada data di {bronze_file}")
                continue
            
            if dataset_name not in quality_rules_config:
                logger.warning(f" Config tidak ditemukan")
                silver_file = bronze_file.replace('bronze', 'silver')
                df_bronze.to_csv(silver_file, index=False)
                silver_files.append(silver_file)
                total_silver += len(df_bronze)
                continue
            
            rules = quality_rules_config[dataset_name]
            
            df_silver, df_quarantine = apply_quality_checks(
                df=df_bronze,
                dataset_name=dataset_name,
                quality_rules=rules
            )
            
            records_pass = len(df_silver)
            records_fail = len(df_quarantine)
            
            if records_pass > 0:
                silver_file = bronze_file.replace('bronze', 'silver')
                df_silver.to_csv(silver_file, index=False)
                silver_files.append(silver_file)
                total_silver += records_pass
                logger.info(f"Silver: {silver_file} ({records_pass} records)")
            
            if records_fail > 0:
                quarantine_file = bronze_file.replace('bronze', 'quarantine')
                df_quarantine.to_csv(quarantine_file, index=False)
                quarantine_files.append(quarantine_file)
                total_quarantine += records_fail
                logger.warning(f"Quarantine: {quarantine_file} ({records_fail} records)")
            
        except Exception as e:
            logger.error(f"Error transforming {dataset_name}: {str(e)}")
            continue
    
    context['task_instance'].xcom_push(key='silver_files', value=silver_files)
    context['task_instance'].xcom_push(key='quarantine_files', value=quarantine_files)
    context['task_instance'].xcom_push(key='quality_stats', value={
        'total_records': total_records,
        'total_silver': total_silver,
        'total_quarantine': total_quarantine
    })
    
    return {
        'silver_files': silver_files,
        'quarantine_files': quarantine_files,
        'stats': {
            'total_records': total_records,
            'total_silver': total_silver,
            'total_quarantine': total_quarantine
        }
    }

# Task ke-3: Load dari silver ke gold 
def task_load_to_gold(**context):

    from utils.gold_loader import load_all_to_gold
    
    try:
        # Path ke folder silver
        silver_path = SILVER_DIR.rstrip('/')
        
        # Load semua ke data mart
        load_all_to_gold(silver_path)

        return "success"
        
    except Exception as e:
        logger.error(f"GOLD LOAD FAILED: {str(e)}")
        raise

# DAG DEFINITION
dag = DAG(
    dag_id='etl_medallion',
    default_args=default_args,
    description='ETL Pipeline untuk data Permohonan KP',
    schedule_interval='*/2 * * * *',  # Jalan setiap 2 menit
    catchup=False,
    tags=['medallion', 'google-sheets'],
)

# TASK DEFINITIONS 
extract = PythonOperator(
    task_id='extract_to_bronze',
    python_callable=task_extract_to_bronze,
    provide_context=True,
    dag=dag,
)

transform_to_silver = PythonOperator(
    task_id='transform_bronze_to_silver',
    python_callable=task_bronze_to_silver,
    provide_context=True,
    dag=dag,
)

load_to_gold = PythonOperator(
    task_id='load_to_gold',
    python_callable=task_load_to_gold,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
extract >> transform_to_silver >> load_to_gold

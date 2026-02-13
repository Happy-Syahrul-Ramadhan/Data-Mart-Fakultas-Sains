"""
Ekstraksi data Permohonan KP dari Google Sheets
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

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
            'asal program studi',
            'jenis permohonan',
            'nama mahasiswa'
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
            'jenis penelitian',
            'jenis permohonan',
            'nama mahasiswa',
            'program studi mahasiswa'
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
            'nama mahasiswa',
            'program studi'
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
            'nama mahasiswa',
            'program studi'
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
            'jenis layanan',
            'nama'
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
            'nama mahasiswa',
            'program studi'
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
            'nama',
            'prodi'
        ],
        'output_path': '/opt/airflow/data/bronze/besaran_ukt_bronze.csv'
    }
]

BRONZE_DIR = '/opt/airflow/data/bronze/'

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
def task_extract_to_bronze(**context):
    """Extract data dari Multiple Google Sheets ke Bronze layer (file terpisah)"""
    
    logger.info("Memulai ekstraksi data dari Multiple Google Sheets")
    logger.info(f"Jumlah sumber: {len(SPREADSHEET_CONFIGS)}")
    
    # Koneksi ke Google Sheets (1x saja untuk semua sheet)
    client = koneksi_google_sheets(CREDENTIALS_PATH)
    
    output_files = []
    
    for idx, config in enumerate(SPREADSHEET_CONFIGS, 1):
        logger.info(f"[{idx}/{len(SPREADSHEET_CONFIGS)}] Ekstraksi: {config['name']}")
        
        try:
            # Ambil data dari Google Sheets
            df = ambil_data_dari_sheet(
                client=client,
                spreadsheet_key=config['spreadsheet_key'],
                sheet_name=config['sheet_name'],
                selected_columns=config['selected_columns']
            )
            
            # Simpan ke CSV
            output_file = simpan_ke_csv(df, config['output_path'])
            output_files.append(output_file)
            
        except Exception as e:
            logger.error(f"Gagal: {str(e)}")
            continue
    
    logger.info(f"Total file berhasil diekstrak: {len(output_files)}")
    context['task_instance'].xcom_push(key='bronze_files', value=output_files)
    return output_files

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

extract
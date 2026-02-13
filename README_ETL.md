# ETL Pipeline - Permohonan KP dengan Medallion Architecture

Pipeline ETL untuk mengambil data permohonan KP dari Google Sheets dan memuat ke MySQL dengan arsitektur Medallion (Bronze-Silver-Gold).

## 📊 Arsitektur Medallion

### Bronze Layer (Raw Data)
- **Sumber**: Google Sheets API
- **Format**: CSV
- **Lokasi**: `data/bronze/`
- **Deskripsi**: Data mentah langsung dari Google Sheets tanpa transformasi

### Silver Layer (Cleaned Data)
- **Sumber**: Bronze Layer
- **Format**: CSV
- **Lokasi**: `data/silver/`
- **Proses**:
  - Standardisasi nama kolom
  - Data cleaning (handling missing values, duplicates)
  - Validasi data quality
  - Type conversion

### Gold Layer (Business-Ready Data)
- **Sumber**: Silver Layer
- **Format**: MySQL Database
- **Lokasi**: Database `gold`
- **Schema**: Star Schema dengan dimensional model
  - `dim_mahasiswa` - Dimensi mahasiswa
  - `dim_status_layanan` - Dimensi status
  - `dim_waktu` - Dimensi waktu
  - `fact_permohonan_kp` - Fakta permohonan

## 🚀 Setup dan Instalasi

### 1. Update Environment Variables

Edit file `.env` dan tambahkan packages yang diperlukan:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=gspread google-auth google-auth-oauthlib google-auth-httplib2 pandas mysql-connector-python
```

### 2. Setup Google Sheets API

1. Buat project di [Google Cloud Console](https://console.cloud.google.com/)
2. Enable Google Sheets API dan Google Drive API
3. Buat Service Account dan download credentials JSON
4. Copy credentials ke `credentials/credentials.json`
5. Share Google Sheets dengan email service account

### 3. Setup MySQL Database

MySQL sudah termasuk dalam docker-compose. Schema akan otomatis dibuat dari file `sql/init_schema.sql`.

### 4. Update Konfigurasi DAG

Edit file `dags/etl_permohonan_kp.py` dan update:

```python
SPREADSHEET_KEY = 'YOUR_SPREADSHEET_ID'  # Dari URL Google Sheets
SHEET_NAME = 'permohonan_kp'
```

Cara mendapatkan SPREADSHEET_KEY:
- URL: `https://docs.google.com/spreadsheets/d/1ABC123xyz/edit`
- SPREADSHEET_KEY: `1ABC123xyz`

### 5. Restart Airflow Services

```powershell
docker-compose down
docker-compose up -d
```

## 📋 Struktur Data

### Kolom di Google Sheets:
1. id
2. status
3. Timestamp
4. asal program studi
5. jenis permohonan
6. nama mahasiswa
7. nim mahasiswa
8. email perwakilan mahasiswa
9. no hp/wa perwakilan mahasiswa
10. tujuan surat dan/instansi tujuan
11. alamat instansi tujuan
12. tanggal pelaksanaan kp
13. dosen pembimbing kp/pkl
14. upload surat permohonan kp dari program studi

### Database Schema (MySQL):

**dim_mahasiswa**
- id_mahasiswa (PK)
- nama_mahasiswa
- NIM (UNIQUE)
- prodi

**dim_status_layanan**
- id_status (PK)
- status_layanan (UNIQUE)

**dim_waktu**
- id_waktu (PK)
- tanggal, jam, hari, bulan, tahun
- nama_hari, nama_bulan

**fact_permohonan_kp**
- id_permohonan (PK)
- id_mahasiswa (FK)
- id_status (FK)
- id_waktu (FK)
- jenis_permohonan, instansi_tujuan, alamat_instansi
- tanggal_pelaksanaan_kp, dosen_pembimbing
- email_mahasiswa, no_hp_mahasiswa
- surat_permohonan_url, timestamp_permohonan

## 🔄 Menjalankan ETL

### Manual Trigger dari Web UI:
1. Buka Airflow Web UI: http://localhost:8080
2. Cari DAG `etl_permohonan_kp_medallion`
3. Toggle ON untuk enable DAG
4. Klik tombol "Play" untuk manual trigger

### Dari Command Line:
```powershell
docker-compose run airflow-worker airflow dags trigger etl_permohonan_kp_medallion
```

### Test Specific Task:
```powershell
# Test extract bronze
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion extract_bronze 2024-01-01

# Test transform silver
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion transform_silver 2024-01-01

# Test load gold
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion load_gold 2024-01-01
```

## 📊 Monitoring

### Melihat Logs ETL:
```powershell
# Logs dari scheduler
docker-compose logs airflow-scheduler -f

# Logs dari worker
docker-compose logs airflow-worker -f
```

### Query MySQL:
```powershell
# Masuk ke MySQL container
docker-compose exec mysql mysql -u root gold

# Query data
SELECT COUNT(*) FROM fact_permohonan_kp;
SELECT * FROM vw_permohonan_kp_lengkap LIMIT 10;
SELECT * FROM vw_statistik_per_prodi;
```

## 🔧 Troubleshooting

### Error: Cannot connect to Google Sheets
- Pastikan `credentials.json` sudah benar
- Pastikan Google Sheets sudah di-share ke service account email
- Pastikan API sudah di-enable di Google Cloud Console

### Error: Cannot connect to MySQL from Airflow
- Gunakan `host.docker.internal` untuk koneksi dari container ke localhost
- Atau tambahkan MySQL ke docker network dan gunakan `mysql` sebagai host

### Error: Module not found
- Pastikan packages sudah di-install via `_PIP_ADDITIONAL_REQUIREMENTS`
- Restart airflow services setelah update `.env`

### Data tidak muncul di MySQL
- Check logs: `docker-compose logs airflow-worker`
- Verifikasi foreign keys di fact table
- Pastikan dimension tables sudah terisi dulu

## 📈 Views untuk Reporting

### View: Laporan Lengkap
```sql
SELECT * FROM vw_permohonan_kp_lengkap;
```

### View: Statistik per Prodi
```sql
SELECT * FROM vw_statistik_per_prodi;
```

### View: Trend Bulanan
```sql
SELECT * FROM vw_trend_permohonan_bulanan;
```

## 🎯 Schedule

Default schedule: `@daily` (setiap hari pukul 00:00)

Untuk mengubah schedule, edit di `dags/etl_permohonan_kp.py`:
```python
schedule_interval='@daily'  # atau '0 0 * * *' untuk cron format
```

Pilihan schedule:
- `@once` - Sekali saja
- `@hourly` - Setiap jam
- `@daily` - Setiap hari
- `@weekly` - Setiap minggu
- `@monthly` - Setiap bulan
- `'0 */6 * * *'` - Setiap 6 jam

## 📁 Struktur File

```
.
├── dags/
│   ├── etl_permohonan_kp.py      # Main DAG file
│   └── example_dag.py             # Example DAG
├── utils/
│   ├── google_sheets_helper.py    # Google Sheets extractor
│   ├── data_transformer.py        # Data transformation
│   └── mysql_loader.py            # MySQL loader
├── data/
│   ├── bronze/                    # Raw data CSV
│   ├── silver/                    # Cleaned data CSV
│   └── gold/                      # (MySQL database)
├── credentials/
│   └── credentials.json           # Google API credentials
├── sql/
│   └── init_schema.sql            # Database schema
└── config/
    └── etl_config.py              # Configuration file
```

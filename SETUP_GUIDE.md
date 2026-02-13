# Cara Setup ETL Pipeline - Quick Start Guide

## 📝 Langkah Setup

### 1. Update Environment Variables
File `.env` sudah diupdate dengan packages yang diperlukan. Restart services:
```powershell
docker-compose down
docker-compose up -d
```

### 2. Setup Google Sheets API Credentials

**A. Buat Project di Google Cloud:**
1. Buka https://console.cloud.google.com/
2. Klik "Select a project" > "New Project"
3. Beri nama project (misal: "Airflow ETL KP")
4. Klik "Create"

**B. Enable APIs:**
1. Di dashboard, klik "Enable APIs and Services"
2. Cari dan enable:
   - Google Sheets API
   - Google Drive API

**C. Buat Service Account:**
1. Navigation Menu > IAM & Admin > Service Accounts
2. Klik "Create Service Account"
3. Beri nama (misal: "airflow-etl")
4. Klik "Create and Continue"
5. Skip roles (optional)
6. Klik "Done"

**D. Download Credentials:**
1. Klik service account yang baru dibuat
2. Tab "Keys" > "Add Key" > "Create new key"
3. Pilih "JSON"
4. Save file sebagai `credentials.json`
5. Copy file ke folder `credentials/credentials.json`

**E. Share Google Sheets:**
1. Buka Google Sheets yang ingin diambil datanya
2. Klik "Share" di kanan atas
3. Paste email service account (format: xxx@xxx.iam.gserviceaccount.com)
4. Set permission: "Viewer" (atau "Editor" jika perlu)
5. Uncheck "Notify people"
6. Klik "Share"

### 3. Update SPREADSHEET_KEY di DAG

**A. Dapatkan Spreadsheet ID:**
- URL Google Sheets: `https://docs.google.com/spreadsheets/d/1ABC123xyz456/edit`
- SPREADSHEET_KEY: `1ABC123xyz456` (bagian setelah `/d/` dan sebelum `/edit`)

**B. Edit file DAG:**
Edit `dags/etl_permohonan_kp.py` baris 23:
```python
SPREADSHEET_KEY = '1ABC123xyz456'  # Ganti dengan ID spreadsheet Anda
```

### 4. Pastikan MySQL Ready

Check MySQL sudah running:
```powershell
docker-compose ps mysql
```

Check schema sudah dibuat:
```powershell
docker-compose exec mysql mysql -u root gold -e "SHOW TABLES;"
```

Expected output:
- dim_mahasiswa
- dim_status_layanan
- dim_waktu
- fact_permohonan_kp

### 5. Test DAG

**A. Check DAG muncul di Airflow:**
1. Buka http://localhost:8080
2. Login: airflow / airflow
3. Cari DAG `etl_permohonan_kp_medallion`
4. Pastikan tidak ada import errors

**B. Manual Trigger:**
1. Toggle DAG menjadi ON
2. Klik tombol "Play" (▶) untuk manual trigger
3. Monitor progress di Graph View

## 🔍 Troubleshooting

### Error: "No module named 'gspread'"
**Solusi:**
```powershell
# Update .env dan restart
docker-compose down
docker-compose up -d
# Tunggu beberapa menit untuk packages terinstall
```

### Error: "Permission denied" di Google Sheets
**Solusi:**
- Pastikan Google Sheets sudah di-share ke service account email
- Cek email di credentials.json > "client_email"

### Error: "Cannot connect to MySQL"
**Solusi:**
- Di dalam container Airflow, gunakan host: `host.docker.internal`
- Atau tambahkan MySQL ke network dan gunakan: `mysql`

Edit `dags/etl_permohonan_kp.py` line 28-33 jika perlu:
```python
MYSQL_CONFIG = {
    'host': 'mysql',  # Ubah jika perlu
    'user': 'root',
    'password': '',
    'database': 'gold'
}
```

### DAG tidak muncul di Airflow UI
**Solusi:**
```powershell
# Check logs
docker-compose logs airflow-scheduler | Select-String "etl_permohonan_kp"

# Check syntax DAG
docker-compose exec airflow-worker python /opt/airflow/dags/etl_permohonan_kp.py
```

## ✅ Checklist Sebelum Run

- [ ] MySQL container running
- [ ] Database schema sudah dibuat (check `SHOW TABLES`)
- [ ] File `credentials/credentials.json` ada dan valid
- [ ] Google Sheets sudah di-share ke service account
- [ ] SPREADSHEET_KEY sudah diupdate di DAG
- [ ] Packages Python sudah terinstall (check logs)
- [ ] DAG muncul di Airflow UI tanpa error
- [ ] DAG sudah di-toggle ON

## 🎯 Test Step by Step

### Step 1: Test Extract (Bronze)
```powershell
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion extract_bronze 2024-01-01
```

Check hasil:
```powershell
# Check file bronze CSV
ls data/bronze/
```

### Step 2: Test Transform (Silver)
```powershell
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion transform_silver 2024-01-01
```

Check hasil:
```powershell
# Check file silver CSV
ls data/silver/
```

### Step 3: Test Load (Gold)
```powershell
docker-compose run airflow-worker airflow tasks test etl_permohonan_kp_medallion load_gold 2024-01-01
```

Check hasil:
```powershell
# Query MySQL
docker-compose exec mysql mysql -u root gold -e "SELECT COUNT(*) FROM fact_permohonan_kp;"
```

### Step 4: Full DAG Run
```powershell
docker-compose run airflow-worker airflow dags test etl_permohonan_kp_medallion 2024-01-01
```

## 📊 Setelah Berhasil

### Query Data di MySQL:
```sql
-- Total permohonan
SELECT COUNT(*) as total FROM fact_permohonan_kp;

-- Laporan lengkap
SELECT * FROM vw_permohonan_kp_lengkap LIMIT 10;

-- Statistik per prodi
SELECT * FROM vw_statistik_per_prodi;

-- Trend bulanan
SELECT * FROM vw_trend_permohonan_bulanan;
```

### Monitor Execution:
- Web UI: http://localhost:8080
- Graph View: Lihat status setiap task
- Logs: Klik task untuk lihat logs detail
- XCom: Lihat metadata yang di-pass antar tasks

Selamat mencoba! 🚀

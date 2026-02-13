# Apache Airflow dengan Docker Compose

Setup Apache Airflow menggunakan Docker Compose untuk development dan testing.

## Struktur Direktori

```
.
├── dags/               # Direktori untuk DAG files
├── logs/               # Direktori untuk logs Airflow
├── plugins/            # Direktori untuk custom plugins
├── config/             # Direktori untuk konfigurasi tambahan
├── docker-compose.yaml # Konfigurasi Docker Compose
├── .env                # Environment variables
└── README.md          # Dokumentasi ini
```

## Prerequisites

- Docker Desktop terinstall dan berjalan
- Minimal 4GB RAM tersedia untuk Docker
- Minimal 2 CPU cores
- Minimal 10GB disk space

## Setup dan Instalasi

### 1. Inisialisasi Airflow

Jalankan perintah berikut untuk menginisialisasi database dan membuat user admin:

```powershell
docker-compose up airflow-init
```

### 2. Start Airflow Services

Setelah inisialisasi selesai, jalankan semua services:

```powershell
docker-compose up -d
```

Atau jika ingin melihat logs secara real-time:

```powershell
docker-compose up
```

### 3. Akses Airflow Web UI

Buka browser dan akses:
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

## Perintah-Perintah Penting

### Melihat Status Services
```powershell
docker-compose ps
```

### Melihat Logs
```powershell
# Semua services
docker-compose logs

# Service tertentu
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Follow logs (real-time)
docker-compose logs -f
```

### Stop Services
```powershell
docker-compose down
```

### Stop dan Hapus Volumes (reset semua data)
```powershell
docker-compose down -v
```

### Restart Services
```powershell
docker-compose restart
```

### Mengakses Airflow CLI
```powershell
docker-compose run airflow-worker airflow dags list
docker-compose run airflow-worker airflow tasks test example_hello_world hello_task 2024-01-01
```

## Komponen Airflow

Setup ini mencakup komponen-komponen berikut:

1. **airflow-webserver** - Web UI (port 8080)
2. **airflow-scheduler** - Scheduler untuk menjalankan DAGs
3. **airflow-worker** - Celery worker untuk menjalankan tasks
4. **airflow-triggerer** - Untuk async tasks
5. **postgres** - Database untuk metadata
6. **redis** - Message broker untuk Celery
7. **flower** (optional) - Monitoring Celery workers (port 5555)

## Menambahkan DAG Baru

1. Buat file Python baru di folder `dags/`
2. File akan otomatis terdeteksi oleh Airflow (perlu beberapa saat)
3. Refresh halaman Web UI untuk melihat DAG baru

## Menginstall Python Packages Tambahan

Edit file `.env` dan tambahkan packages yang dibutuhkan:

```
_PIP_ADDITIONAL_REQUIREMENTS=pandas==1.5.3 numpy==1.24.2 requests
```

Kemudian restart services:

```powershell
docker-compose down
docker-compose up -d
```

## Monitoring dengan Flower (Optional)

Untuk mengaktifkan Flower monitoring tool:

```powershell
docker-compose --profile flower up -d
```

Akses Flower UI di: http://localhost:5555

## Troubleshooting

### Port sudah digunakan
Jika port 8080 sudah digunakan, edit `docker-compose.yaml` dan ubah port mapping:
```yaml
ports:
  - "8081:8080"  # Ubah port host menjadi 8081
```

### Permission Issues
Jika ada masalah permission di Windows, pastikan Docker Desktop memiliki akses ke drive D:

### Memory Issues
Jika Docker kehabisan memory, tingkatkan alokasi memory di Docker Desktop Settings.

### DAG tidak muncul
- Periksa apakah ada error di logs: `docker-compose logs airflow-scheduler`
- Pastikan file DAG tidak ada syntax error
- Tunggu beberapa saat (Airflow scan DAG setiap 30 detik secara default)

## Referensi

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Docker Compose Setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Writing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)

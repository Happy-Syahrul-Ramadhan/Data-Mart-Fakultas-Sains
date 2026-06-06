# Arsitektur Data Mart Fakultas Sains
<img width="1231" height="773" alt="Image" src="https://github.com/user-attachments/assets/6cf79fa3-a7b1-4b97-a1d2-f95db78a2129" />

Proyek ini adalah pipeline ETL berbasis Apache Airflow untuk mengolah data permohonan layanan mahasiswa ke data mart PostgreSQL.

## Gambaran Singkat

Pipeline memakai arsitektur medallion:
- **Bronze**: data mentah dari Google Sheets disimpan ke CSV.
- **Silver**: data divalidasi, dibersihkan, dan dipisahkan ke data layak pakai atau quarantine.
- **Gold**: data dimuat ke PostgreSQL sebagai tabel dimensi dan tabel fakta untuk analisis.
- **Visualisasi**: Apache Superset dapat dihubungkan ke PostgreSQL untuk dashboard dan eksplorasi data.

DAG utama bernama `etl_medallion` dan dijalankan otomatis setiap 2 menit.

## Alur Proses

1. Airflow mengambil data dari beberapa spreadsheet Google Sheets.
2. Data mentah disimpan ke folder `data/bronze/`.
3. Aturan kualitas diterapkan untuk validasi kolom, format, nilai, dan duplikasi.
4. Data lolos validasi disimpan ke `data/silver/`, sedangkan data gagal masuk ke `data/quarantine/`.
5. Data silver dimuat ke PostgreSQL ke schema `datamart`.

## Arsitektur Sistem

```mermaid
flowchart LR
    A[Google Sheets] --> B[Airflow DAG etl_medallion]
    B --> C[Bronze CSV]
    C --> D[Quality Check]
    D --> E[Silver CSV]
    D --> F[Quarantine CSV]
    E --> G[Gold Loader]
    G --> H[(PostgreSQL Datamart)]
    H --> I[Apache Superset Dashboard]

    subgraph Docker Compose
        I[Airflow Webserver]
        J[Airflow Scheduler]
        K[Airflow Worker]
        L[Airflow Triggerer]
        M[Redis]
        N[PostgreSQL]
    end
```

### Visualisasi dengan Apache Superset

Apache Superset digunakan sebagai layer business intelligence untuk membuat dashboard dari tabel di schema `datamart`.

pada dashboard mencakup 3 KPI dan beberapa visualisasi lainnya:
- jumlah permohonan per jenis layanan
- status layanan per periode waktu
- distribusi permohonan per program studi
- tren layanan mahasiswa dari waktu ke waktu

Superset terhubung langsung ke PostgreSQL sehingga data yang sudah masuk ke layer gold bisa dianalisis tanpa mengubah pipeline ETL.

### Komponen Utama

- **Apache Airflow**: orkestrasi ETL, scheduling, dan monitoring DAG.
- **Google Sheets API**: sumber data utama.
- **Pandas**: transformasi dan validasi data.
- **PostgreSQL**: penyimpanan layer gold / datamart.
- **Apache Superset**: visualisasi dashboard dan analisis data dari datamart.
- **Redis**: broker untuk Celery Executor.
- **Docker Compose**: menjalankan seluruh stack secara lokal.

## Struktur Proyek

- `dags/` - definisi DAG Airflow.
- `utils/` - helper koneksi database, quality rules, dan gold loader.
- `config/` - schema datamart dan konfigurasi webserver.
- `data/` - file CSV bronze, silver, dan quarantine.
- `credentials/` - kredensial Google Sheets.
- `logs/` - log eksekusi Airflow.

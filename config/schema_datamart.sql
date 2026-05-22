-- Buat schema datamart
CREATE SCHEMA IF NOT EXISTS datamart;

-- 1. Dimensi Status Layanan
-- Menyimpan jenis status: Pending, Sudah Dilayani
CREATE TABLE IF NOT EXISTS datamart.dim_status_layanan (
    id_status SERIAL PRIMARY KEY,
    status_layanan VARCHAR(50) NOT NULL UNIQUE
);

-- 2. Dimensi Jenis Layanan
-- Menyimpan jenis layanan: Kerja Praktik, Magang, dll
CREATE TABLE IF NOT EXISTS datamart.dim_layanan_jenis (
    id_layanan_jenis SERIAL PRIMARY KEY,
    nama_layanan VARCHAR(100) NOT NULL UNIQUE
);

-- 3. Dimensi Mahasiswa
-- Menyimpan data mahasiswa 
CREATE TABLE IF NOT EXISTS datamart.dim_mahasiswa (
    id_mahasiswa SERIAL PRIMARY KEY,
    nim VARCHAR(50) NOT NULL UNIQUE,
    nama_mahasiswa VARCHAR(200) NOT NULL,
    program_studi VARCHAR(200) NOT NULL
);

-- 4. Dimensi Waktu
-- Menyimpan dimensi waktu untuk analisis time-series
CREATE TABLE IF NOT EXISTS datamart.dim_waktu (
    id_waktu SERIAL PRIMARY KEY,
    tanggal DATE NOT NULL,
    jam TIME,
    hari VARCHAR(20),
    bulan VARCHAR(20),
    tahun INT,
    hour INT,
    UNIQUE(tanggal, jam)
);

-- Menyimpan transaksi pengajuan layanan dalam bentuk denormalized
CREATE TABLE IF NOT EXISTS datamart.fact_layanan_mahasiswa (
    id_fact_layanan SERIAL PRIMARY KEY,
    id_mahasiswa INT NOT NULL,
    nim VARCHAR(50) NOT NULL,
    nama_mahasiswa VARCHAR(200) NOT NULL,
    program_studi VARCHAR(200) NOT NULL,
    id_status INT NOT NULL,
    status_layanan VARCHAR(50) NOT NULL,
    id_layanan_jenis INT NOT NULL,
    nama_layanan VARCHAR(100) NOT NULL,
    id_waktu INT NOT NULL,
    tanggal DATE NOT NULL,
    jam TIME,
    hari VARCHAR(20),
    bulan VARCHAR(20),
    tahun INT,
    hour INT,
    total_layanan_masuk INT DEFAULT 1,
    total_layanan_pending INT DEFAULT 0,
    total_layanan_sudah_dilayani INT DEFAULT 0,
    FOREIGN KEY (id_mahasiswa) REFERENCES datamart.dim_mahasiswa(id_mahasiswa),
    FOREIGN KEY (id_status) REFERENCES datamart.dim_status_layanan(id_status),
    FOREIGN KEY (id_layanan_jenis) REFERENCES datamart.dim_layanan_jenis(id_layanan_jenis),
    FOREIGN KEY (id_waktu) REFERENCES datamart.dim_waktu(id_waktu)
);
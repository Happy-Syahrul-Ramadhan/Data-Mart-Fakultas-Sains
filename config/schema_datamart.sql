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
    nim VARCHAR(50) PRIMARY KEY,
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

-- Menyimpan transaksi pengajuan layanan dengan composite primary key
CREATE TABLE IF NOT EXISTS datamart.fact_layanan_mahasiswa (
    nim VARCHAR(50) NOT NULL,
    id_status INT NOT NULL,
    id_layanan_jenis INT NOT NULL,
    id_waktu INT NOT NULL,
    PRIMARY KEY (nim, id_status, id_layanan_jenis, id_waktu),
    FOREIGN KEY (nim) REFERENCES datamart.dim_mahasiswa(nim),
    FOREIGN KEY (id_status) REFERENCES datamart.dim_status_layanan(id_status),
    FOREIGN KEY (id_layanan_jenis) REFERENCES datamart.dim_layanan_jenis(id_layanan_jenis),
    FOREIGN KEY (id_waktu) REFERENCES datamart.dim_waktu(id_waktu)
);

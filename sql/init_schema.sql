-- SQL Schema untuk Database Gold (Dimensional Model)
-- Database: gold

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- Tabel Dimensi Mahasiswa
CREATE TABLE IF NOT EXISTS dim_mahasiswa (
    id_mahasiswa INT AUTO_INCREMENT PRIMARY KEY,
    nama_mahasiswa VARCHAR(255) NOT NULL,
    NIM VARCHAR(50) NOT NULL UNIQUE,
    prodi VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_nim (NIM),
    INDEX idx_prodi (prodi)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel Dimensi Jenis Layanan
CREATE TABLE IF NOT EXISTS dim_jenis_layanan (
    id_jenis_layanan INT AUTO_INCREMENT PRIMARY KEY,
    jenis_layanan VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_jenis_layanan (jenis_layanan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel Dimensi Waktu
CREATE TABLE IF NOT EXISTS dim_waktu (
    id_waktu INT AUTO_INCREMENT PRIMARY KEY,
    tanggal INT NOT NULL,
    jam INT NOT NULL,
    hari INT NOT NULL,
    bulan INT NOT NULL,
    nama_hari VARCHAR(20) NOT NULL,
    nama_bulan VARCHAR(20) NOT NULL,
    tahun INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tanggal (tanggal, bulan, tahun),
    INDEX idx_tahun_bulan (tahun, bulan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- FACT TABLE
-- =====================================================

-- Tabel Fakta Layanan (Simplified Star Schema)
CREATE TABLE IF NOT EXISTS fact_layanan (
    id_layanan INT AUTO_INCREMENT PRIMARY KEY,
    id_mahasiswa INT NOT NULL,
    id_jenis_layanan INT NOT NULL,
    id_waktu INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (id_mahasiswa) REFERENCES dim_mahasiswa(id_mahasiswa) ON DELETE CASCADE,
    FOREIGN KEY (id_jenis_layanan) REFERENCES dim_jenis_layanan(id_jenis_layanan) ON DELETE CASCADE,
    FOREIGN KEY (id_waktu) REFERENCES dim_waktu(id_waktu) ON DELETE CASCADE,
    
    -- Indexes untuk performance
    INDEX idx_mahasiswa (id_mahasiswa),
    INDEX idx_jenis_layanan (id_jenis_layanan),
    INDEX idx_waktu (id_waktu)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- VIEWS untuk Reporting (Optional)
-- =====================================================

-- View: Laporan Layanan Lengkap
CREATE OR REPLACE VIEW vw_layanan_lengkap AS
SELECT 
    f.id_layanan,
    m.nama_mahasiswa,
    m.NIM,
    m.prodi,
    j.jenis_layanan,
    w.tanggal,
    w.nama_hari,
    w.bulan,
    w.nama_bulan,
    w.tahun,
    f.created_at
FROM fact_layanan f
INNER JOIN dim_mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
INNER JOIN dim_jenis_layanan j ON f.id_jenis_layanan = j.id_jenis_layanan
INNER JOIN dim_waktu w ON f.id_waktu = w.id_waktu
ORDER BY f.created_at DESC;

-- View: Statistik Layanan per Prodi
CREATE OR REPLACE VIEW vw_statistik_per_prodi AS
SELECT 
    m.prodi,
    COUNT(f.id_layanan) as jumlah_layanan,
    COUNT(DISTINCT m.id_mahasiswa) as jumlah_mahasiswa,
    j.jenis_layanan,
    COUNT(*) as jumlah_per_jenis
FROM fact_layanan f
INNER JOIN dim_mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
INNER JOIN dim_jenis_layanan j ON f.id_jenis_layanan = j.id_jenis_layanan
GROUP BY m.prodi, j.jenis_layanan
ORDER BY m.prodi, jumlah_layanan DESC;

-- View: Trend Layanan per Bulan
CREATE OR REPLACE VIEW vw_trend_layanan_bulanan AS
SELECT 
    w.tahun,
    w.bulan,
    w.nama_bulan,
    COUNT(f.id_layanan) as jumlah_layanan,
    COUNT(DISTINCT f.id_mahasiswa) as jumlah_mahasiswa_unik
FROM fact_layanan f
INNER JOIN dim_waktu w ON f.id_waktu = w.id_waktu
GROUP BY w.tahun, w.bulan, w.nama_bulan
ORDER BY w.tahun DESC, w.bulan DESC;

-- =====================================================
-- Insert Data Status Default (Optional)
-- =====================================================

INSERT IGNORE INTO dim_jenis_layanan (jenis_layanan) VALUES
('Pending'),
('Diproses'),
('Disetujui'),
('Ditolak'),
('Selesai');

-- =====================================================
-- Comments untuk Documentation
-- =====================================================

-- Dimension Tables:
-- - dim_mahasiswa: Menyimpan informasi mahasiswa (SCD Type 1)
-- - dim_jenis_layanan: Menyimpan jenis layanan (boleh duplikat)
-- - dim_waktu: Menyimpan dimensi waktu untuk analisis temporal (boleh duplikat)

-- Fact Table:
-- - fact_layanan: Menyimpan relasi layanan mahasiswa (simplified fact table)
--   Hanya menyimpan foreign keys ke dimension tables untuk analisis relasional
--   Unique constraint pada kombinasi keseluruhan (id_mahasiswa, id_jenis_layanan, id_waktu)

-- Schema ini mengikuti Simplified Star Schema design pattern untuk Data Warehouse

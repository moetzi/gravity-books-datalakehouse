# Proyek Akhir Data Lakehouse: Gravity Books📖

Proyek ini merupakan implementasi dan perbandingan dua arsitektur data analitik: Data Warehouse tradisional dan Data Lakehouse modern, menggunakan studi kasus data penjualan dari toko buku online "Gravity Books".

## Ringkasan Proyek

Tujuan utama dari proyek ini adalah untuk membangun dua pipeline ETL (Extract, Transform, Load) secara paralel untuk menjawab pertanyaan bisnis yang sama. Proyek ini mencakup:

1.  **Desain Skema Bintang:** Merancang model data dimensional yang terdiri dari 4 tabel dimensi dan 1 tabel fakta.
2.  **Implementasi Data Warehouse:** Membangun pipeline ETL menggunakan Python untuk memuat data ke dalam skema bintang di database **MySQL**.
3.  **Implementasi Data Lakehouse:** Membangun pipeline ETL dengan **arsitektur Medallion** (Bronze, Silver, Gold) di mana data akhir disimpan dalam format **Parquet** di *object storage* **MinIO**.
4.  **Analisis Komparatif:** Menganalisis dan membandingkan kinerja kedua arsitektur berdasarkan metrik waktu proses ETL dan penggunaan ruang penyimpanan.
5.  **Validasi Data:** Melakukan uji coba dengan 5 kueri analitik yang berbeda pada kedua sistem untuk memvalidasi bahwa keduanya menghasilkan data yang identik.

---

## Tech Stack yang Digunakan

-   **Bahasa Pemrograman:** Python 3.10
-   **Library Inti:** Pandas, SQLAlchemy, PyArrow
-   **Database Sumber & DWH:** MySQL
-   **Object Storage (Lakehouse):** MinIO
-   **Format Data Lakehouse:** Apache Parquet
-   **Query Engine (untuk Lakehouse):** DuckDB
-   **Lingkungan:** WSL (Windows Subsystem for Linux) - Ubuntu

---

## Arsitektur Final

Proyek ini menghasilkan dua output akhir dari satu sumber data yang sama:

1.  **Database `gravity_books_dwh` di MySQL:** Berisi skema bintang yang siap di-query menggunakan SQL.
2.  **Folder `gold/` di MinIO:** Berisi file-file Parquet yang merepresentasikan skema bintang dan siap di-query oleh *engine* seperti DuckDB atau Spark.

---

## Cara Menjalankan Proyek

Berikut adalah langkah-langkah untuk menyiapkan dan menjalankan keseluruhan proyek dari awal.

### 1. Prasyarat

-   Pastikan **Git**, **Python 3.10+**, **MySQL Server**, dan **MinIO Server** sudah terinstal di lingkungan Anda.

### 2. Kloning Repositori

```bash
git clone https://github.com/moetzi/gravity-books-datalakehouse.git
cd gravity-books-datalakehouse
```

### 3. Setup Lingkungan Python
```bash
# Buat virtual environment
python3.10 -m venv venv

# Aktifkan virtual environment
source venv/bin/activate

# Instal semua library yang dibutuhkan
pip install -r requirements.txt
```

### 4. Setup Database & Data Sumber
- Login ke MySQL dan jalankan semua skrip .sql yang ada di dalam folder source_sql/ secara berurutan dari 01 hingga 13. Ini akan membuat dan mengisi database sumber `gravity_books`.

- Buat sebuah database kosong baru bernama `gravity_books_dwh`.

- Jalankan skrip SQL untuk membuat semua tabel di DWH.

### 5. Jalankan Pipeline ETL
> **PENTING** : Sebelum menjalankan, buka setiap file skrip Python dan ganti placeholder username dan password database sesuai dengan kredensial MySQL Anda.

Menjalankan Pipeline Data Warehouse:

```bash
python3.10 etl_script_dwh/etl_dwh.py
```

Menjalankan Pipeline Data Lakehouse:
> Pastikan server MinIO Anda berjalan dan bucket sudah dibuat

```bash
python3.10 etl_script_dlh/etl_lakehouse.py
```

### 6. Jalankan Skrip Validasi Kueri
Skrip ini akan menjalankan 5 kueri analitik pada kedua sistem dan membandingkan hasilnya.

```bash
python3.10 test_queries/test_queries.py
```

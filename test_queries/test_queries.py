import pandas as pd
from sqlalchemy import create_engine
import duckdb
import time

# --- 1. KONFIGURASI ---
# Ganti 'password' dengan password root MySQL Anda
DWH_DB_URL = 'mysql+mysqlconnector://USERNAME:PASSWORD@localhost/gravity_books_dwh'
MINIO_ENDPOINT = '172.31.171.46:9000'
MINIO_ACCESS_KEY = 'USERNAME'
MINIO_SECRET_KEY = 'PASSWORD'
BUCKET_NAME = 'gravity-books-lake'

# --- 2. DEFINISI KUERI ANALITIK ---
queries = {
    "1_penjualan_per_bulan": """
        SELECT 
            d.year_val,
            d.month_name,
            COUNT(f.book_sk) AS total_buku_terjual,
            SUM(f.price) AS total_penjualan
        FROM fact_book_sales f
        JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.year_val, d.month_val, d.month_name
        ORDER BY d.year_val, d.month_val;
    """,
    "2_top_10_buku_terlaris": """
        SELECT
            b.title,
            b.author_name,
            COUNT(f.book_sk) AS jumlah_terjual
        FROM fact_book_sales f
        JOIN dim_book b ON f.book_sk = b.book_sk
        GROUP BY b.book_sk, b.title, b.author_name
        ORDER BY jumlah_terjual DESC, b.title ASC
        LIMIT 10;
    """,
    "3_top_5_pelanggan_setia": """
        SELECT
            c.first_name,
            c.last_name,
            c.email,
            SUM(f.price + f.shipping_cost) AS total_belanja
        FROM fact_book_sales f
        JOIN dim_customer c ON f.customer_sk = c.customer_sk
        GROUP BY c.customer_sk, c.first_name, c.last_name, c.email
        ORDER BY total_belanja DESC, c.customer_sk ASC
        LIMIT 5;
    """,
    "4_penjualan_berdasarkan_negara": """
        SELECT
            c.country,
            SUM(f.price) AS total_penjualan
        FROM fact_book_sales f
        JOIN dim_customer c ON f.customer_sk = c.customer_sk
        GROUP BY c.country
        ORDER BY total_penjualan DESC, c.country ASC;
    """,
    "5_metode_pengiriman_terpopuler": """
        SELECT
            s.shipping_method,
            COUNT(*) AS jumlah_penggunaan
        FROM fact_book_sales f
        JOIN dim_shipping s ON f.shipping_sk = s.shipping_sk
        GROUP BY s.shipping_method
        ORDER BY jumlah_penggunaan DESC, s.shipping_method ASC;
    """
}

def run_queries_on_dwh(query_sql):
    """Menjalankan kueri di DWH MySQL menggunakan koneksi baru."""
    try:
        engine = create_engine(DWH_DB_URL)
        with engine.connect() as connection:
            df = pd.read_sql_query(query_sql, connection)
            return df
    except Exception as e:
        print(f"  - Gagal menjalankan kueri di DWH: {e}")
        return None

def run_queries_on_lakehouse(query_sql):
    """Menjalankan kueri di Lakehouse (MinIO) menggunakan DuckDB."""
    try:
        con = duckdb.connect(database=':memory:', read_only=False)
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}';")
        con.execute("SET s3_url_style = 'path'; SET s3_use_ssl = false;")
        con.execute(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}';")

        con.execute(f"CREATE OR REPLACE VIEW fact_book_sales AS SELECT * FROM 's3://{BUCKET_NAME}/gold/fact_book_sales.parquet';")
        con.execute(f"CREATE OR REPLACE VIEW dim_date AS SELECT * FROM 's3://{BUCKET_NAME}/gold/dim_date.parquet';")
        con.execute(f"CREATE OR REPLACE VIEW dim_customer AS SELECT * FROM 's3://{BUCKET_NAME}/gold/dim_customer.parquet';")
        con.execute(f"CREATE OR REPLACE VIEW dim_book AS SELECT * FROM 's3://{BUCKET_NAME}/gold/dim_book.parquet';")
        con.execute(f"CREATE OR REPLACE VIEW dim_shipping AS SELECT * FROM 's3://{BUCKET_NAME}/gold/dim_shipping.parquet';")

        result_df = con.execute(query_sql).fetchdf()
        return result_df
    except Exception as e:
        print(f"  - Gagal menjalankan kueri di Lakehouse: {e}")
        return None

def compare_dataframes(df1, df2):
    """Membandingkan dua DataFrame, lebih toleran terhadap tipe data dan urutan."""
    if df1 is None or df2 is None: return False
    if df1.shape != df2.shape: return False
    if df1.empty and df2.empty: return True

    df1_comp = df1.copy().reset_index(drop=True)
    df2_comp = df2.copy().reset_index(drop=True)
    
    for col in df1_comp.columns:
        if pd.api.types.is_numeric_dtype(df1_comp[col]):
            df1_comp[col] = df1_comp[col].round(4)
    for col in df2_comp.columns:
        if pd.api.types.is_numeric_dtype(df2_comp[col]):
            df2_comp[col] = df2_comp[col].round(4)
            
    return df1_comp.astype(str).equals(df2_comp.astype(str))

if __name__ == '__main__':
    print("--- Memulai Uji Coba Kueri ---")
    all_results_match = True
    
    for name, sql in queries.items():
        print(f"\n=======================================================")
        print(f"INFO: Menjalankan Kueri: {name}")
        print(f"=======================================================")
        
        # --- Jalankan di DWH ---
        print("\n----- Hasil dari Data Warehouse (MySQL) -----")
        start_dwh = time.time()
        dwh_result = run_queries_on_dwh(sql)
        end_dwh = time.time()
        print(dwh_result) # <-- PERUBAHAN: Selalu tampilkan hasil
        print(f"(Selesai dalam {end_dwh - start_dwh:.4f} detik.)")

        # --- Jalankan di Lakehouse ---
        print("\n----- Hasil dari Data Lakehouse (DuckDB on MinIO) -----")
        start_dlh = time.time()
        dlh_result = run_queries_on_lakehouse(sql)
        end_dlh = time.time()
        print(dlh_result) # <-- PERUBAHAN: Selalu tampilkan hasil
        print(f"(Selesai dalam {end_dlh - start_dlh:.4f} detik.)")
        
        # --- Bandingkan hasil ---
        are_same = compare_dataframes(dwh_result, dlh_result)
        
        if are_same:
            print("\n[OK] Hasil dari kedua sistem SAMA.")
        else:
            print("\n[ERROR] Hasil dari kedua sistem BERBEDA.")
            all_results_match = False

    print("\n\n--- Uji Coba Kueri Selesai ---")
    if all_results_match:
        print("\n[SUCCESS] SEMUA HASIL KUERI DARI KEDUA SISTEM COCOK!")
    else:
        print("\n[FAILURE] Terdapat perbedaan pada hasil kueri.")

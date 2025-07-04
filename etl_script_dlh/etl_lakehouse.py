import pandas as pd
from sqlalchemy import create_engine
import s3fs
import time

# --- 1. KONFIGURASI ---
# Ganti 'password' dengan password root MySQL Anda
SOURCE_DB_URL = 'mysql+mysqlconnector://USERNAME:PASSWORD@localhost/gravity_books'
MINIO_ENDPOINT = '172.31.171.46:9000'
MINIO_ACCESS_KEY = 'USERNAME'
MINIO_SECRET_KEY = 'PASSWORD'
BUCKET_NAME = 'gravity-books-lake'

storage_options = {
    'key': MINIO_ACCESS_KEY,
    'secret': MINIO_SECRET_KEY,
    'client_kwargs': {'endpoint_url': f'http://{MINIO_ENDPOINT}'}
}
source_engine = create_engine(SOURCE_DB_URL)

def prepare_lakehouse_layers():
    """Mengosongkan semua direktori layer di MinIO."""
    print("Mempersiapkan layer di MinIO (menghapus data lama)...")
    try:
        s3 = s3fs.S3FileSystem(key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY, client_kwargs={'endpoint_url': f'http://{MINIO_ENDPOINT}'})
        layers = ['bronze', 'silver', 'gold']
        for layer in layers:
            layer_path = f'{BUCKET_NAME}/{layer}'
            if s3.exists(layer_path):
                s3.rm(layer_path, recursive=True)
                print(f"  - Direktori '{layer}' berhasil dihapus.")
            s3.mkdir(layer_path)
            print(f"  - Direktori '{layer}' berhasil dibuat kembali.")
    except Exception as e:
        print(f"Gagal mempersiapkan layer di MinIO. Error: {e}")

def run_bronze_layer():
    """LAYER BRONZE: Ekstrak data mentah 1:1 dari sumber ke MinIO."""
    print("\n--- Memulai Bronze Layer ---")
    tables_to_extract = ['author', 'book', 'book_author', 'book_language', 'publisher', 'address', 'country', 'customer', 'customer_address', 'address_status', 'cust_order', 'order_line', 'shipping_method']
    for table in tables_to_extract:
        df = pd.read_sql_table(table, source_engine)
        df.to_parquet(f's3://{BUCKET_NAME}/bronze/{table}.parquet', storage_options=storage_options, index=False)
        print(f"  - Berhasil menyimpan {table}.parquet ke Bronze Layer.")
    print("--- Bronze Layer Selesai ---")

def run_silver_layer():
    """LAYER SILVER: Membersihkan dan mentransformasi data dari Bronze."""
    print("\n--- Memulai Silver Layer ---")
    bronze_files = ['author', 'book', 'book_author', 'book_language', 'publisher', 'address', 'country', 'customer', 'customer_address', 'address_status', 'cust_order', 'order_line', 'shipping_method']
    data = {f: pd.read_parquet(f's3://{BUCKET_NAME}/bronze/{f}.parquet', storage_options=storage_options) for f in bronze_files}

    # 1. Transformasi Data Customer
    df_cust_link = pd.merge(data['customer'], data['customer_address'], on='customer_id')
    df_cust_addr = pd.merge(df_cust_link, data['address'], on='address_id')
    df_cust_country = pd.merge(df_cust_addr, data['country'], on='country_id')
    cleaned_customer = pd.merge(df_cust_country, data['address_status'], on='status_id')
    cleaned_customer.to_parquet(f's3://{BUCKET_NAME}/silver/cleaned_customer.parquet', storage_options=storage_options, index=False)
    print("  - Berhasil menyimpan cleaned_customer.parquet ke Silver Layer.")

    # 2. Transformasi Data Buku
    df_b_pub = pd.merge(data['book'], data['publisher'], on='publisher_id', how='left')
    df_b_pub_lang = pd.merge(df_b_pub, data['book_language'], on='language_id', how='left')
    df_authors = pd.merge(data['book_author'], data['author'], on='author_id').groupby('book_id')['author_name'].apply(', '.join).reset_index()
    cleaned_book = pd.merge(df_b_pub_lang, df_authors, on='book_id', how='left')
    cleaned_book.to_parquet(f's3://{BUCKET_NAME}/silver/cleaned_book.parquet', storage_options=storage_options, index=False)
    print("  - Berhasil menyimpan cleaned_book.parquet ke Silver Layer.")

    # 3. Transformasi Data Pesanan
    cleaned_order = pd.merge(data['order_line'], data['cust_order'], on='order_id')
    cleaned_order = pd.merge(cleaned_order, data['shipping_method'], left_on='shipping_method_id', right_on='method_id', how='left')
    cleaned_order.to_parquet(f's3://{BUCKET_NAME}/silver/cleaned_order.parquet', storage_options=storage_options, index=False)
    print("  - Berhasil menyimpan cleaned_order.parquet ke Silver Layer.")
    print("--- Silver Layer Selesai ---")

def run_gold_layer():
    """LAYER GOLD: Membentuk skema bintang dari data di Silver Layer."""
    print("\n--- Memulai Gold Layer ---")
    
    customer_df = pd.read_parquet(f's3://{BUCKET_NAME}/silver/cleaned_customer.parquet', storage_options=storage_options)
    book_df = pd.read_parquet(f's3://{BUCKET_NAME}/silver/cleaned_book.parquet', storage_options=storage_options)
    order_df = pd.read_parquet(f's3://{BUCKET_NAME}/silver/cleaned_order.parquet', storage_options=storage_options)
    
    # 1. Buat dan Simpan Dimensi Final
    # DimCustomer
    dim_customer = customer_df[['customer_id', 'first_name', 'last_name', 'email', 'address_status', 'street_number', 'street_name', 'city', 'country_name']].copy()
    dim_customer.rename(columns={'country_name': 'country'}, inplace=True)
    dim_customer.insert(0, 'customer_sk', range(1, 1 + len(dim_customer)))
    
    # DimBook
    dim_book = book_df[['book_id', 'title', 'isbn13', 'language_code', 'language_name', 'num_pages', 'publication_date', 'publisher_name', 'author_name']].copy()
    dim_book.insert(0, 'book_sk', range(1, 1 + len(dim_book)))

    # DimShipping
    dim_shipping = order_df[['shipping_method_id', 'method_name']].drop_duplicates().copy()
    dim_shipping.rename(columns={'method_name': 'shipping_method'}, inplace=True)
    dim_shipping.insert(0, 'shipping_sk', range(1, 1 + len(dim_shipping)))
    
    # DimDate (PERBAIKAN LOGIKA)
    min_date = order_df['order_date'].min()
    max_date = order_df['order_date'].max()
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    dim_date = pd.DataFrame(data=date_range, columns=['full_date'])
    dim_date['date_sk'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['day_val'] = dim_date['full_date'].dt.day
    dim_date['month_val'] = dim_date['full_date'].dt.month
    dim_date['year_val'] = dim_date['full_date'].dt.year
    dim_date['quarter_val'] = dim_date['full_date'].dt.quarter
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    
    # Simpan semua dimensi ke Gold Layer
    dim_customer.to_parquet(f's3://{BUCKET_NAME}/gold/dim_customer.parquet', storage_options=storage_options, index=False)
    dim_book.to_parquet(f's3://{BUCKET_NAME}/gold/dim_book.parquet', storage_options=storage_options, index=False)
    dim_shipping.to_parquet(f's3://{BUCKET_NAME}/gold/dim_shipping.parquet', storage_options=storage_options, index=False)
    dim_date.to_parquet(f's3://{BUCKET_NAME}/gold/dim_date.parquet', storage_options=storage_options, index=False)
    print("  - Berhasil menyimpan semua tabel dimensi ke Gold Layer.")
    
    # 2. Buat dan Simpan Tabel Fakta
    fact_merged = pd.merge(order_df, dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    fact_merged = pd.merge(fact_merged, dim_book[['book_sk', 'book_id']], on='book_id', how='left')
    fact_merged = pd.merge(fact_merged, dim_shipping[['shipping_sk', 'shipping_method_id']], on='shipping_method_id', how='left')
    
    fact_merged['order_date'] = pd.to_datetime(fact_merged['order_date']).dt.date
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date
    fact_merged = pd.merge(fact_merged, dim_date[['date_sk', 'full_date']], left_on='order_date', right_on='full_date', how='left')

    fact_book_sales = fact_merged[['customer_sk', 'book_sk', 'shipping_sk', 'date_sk', 'price', 'cost']].copy()
    fact_book_sales.rename(columns={'cost': 'shipping_cost'}, inplace=True)
    fact_book_sales.dropna(inplace=True)
    sk_columns = ['customer_sk', 'book_sk', 'shipping_sk', 'date_sk']
    fact_book_sales[sk_columns] = fact_book_sales[sk_columns].astype(int)

    fact_book_sales.to_parquet(f's3://{BUCKET_NAME}/gold/fact_book_sales.parquet', storage_options=storage_options, index=False)
    print("  - Berhasil menyimpan tabel fakta ke Gold Layer.")
    print("--- Gold Layer Selesai ---")

if __name__ == '__main__':
    total_start_time = time.time()
    
    prepare_lakehouse_layers()
    run_bronze_layer()
    run_silver_layer()
    run_gold_layer()

    total_end_time = time.time()
    print(f"\nPipeline ETL Lakehouse selesai dalam {total_end_time - total_start_time:.2f} detik.")

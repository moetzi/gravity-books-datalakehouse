import pandas as pd
from sqlalchemy import create_engine, text
import time

# --- 1. KONFIGURASI ---
# Ganti 'password' dengan password root MySQL Anda
SOURCE_DB_URL = 'mysql+mysqlconnector://USERNAME:PASSWORD@localhost/gravity_books'
DWH_DB_URL = 'mysql+mysqlconnector://USERNAME:PASSWORD@localhost/gravity_books_dwh'

# Buat koneksi engine menggunakan SQLAlchemy
source_engine = create_engine(SOURCE_DB_URL)
dwh_engine = create_engine(DWH_DB_URL)


def prepare_dwh_tables():
    """Mengosongkan semua tabel di DWH sebelum memuat data baru."""
    print("Mempersiapkan tabel DWH (mengosongkan data lama)...")
    dwh_tables = ['fact_book_sales', 'dim_date', 'dim_customer', 'dim_shipping', 'dim_book']
    with dwh_engine.connect() as connection:
        with connection.begin():
            connection.execute(text('SET FOREIGN_KEY_CHECKS = 0;'))
            for table in dwh_tables:
                try:
                    connection.execute(text(f'TRUNCATE TABLE {table};'))
                    print(f"  - Tabel {table} berhasil dikosongkan.")
                except Exception as e:
                    print(f"  - Peringatan saat mengosongkan {table}: {e}")
            connection.execute(text('SET FOREIGN_KEY_CHECKS = 1;'))
    print("Tabel DWH siap untuk dimuat.")


def extract_data():
    """Mengekstrak data dari semua tabel yang diperlukan dari database sumber."""
    print("Memulai proses ekstraksi data...")
    tables = [
        'author', 'book', 'book_author', 'book_language', 'publisher', 
        'address', 'country', 'customer', 'customer_address', 
        'address_status', 'cust_order', 'order_line', 'shipping_method'
    ]
    data_frames = {}
    for table in tables:
        data_frames[table] = pd.read_sql_table(table, source_engine)
        print(f"  - Berhasil mengekstrak tabel: {table}")
    print("Proses ekstraksi data selesai.")
    return data_frames

def transform_dimensions(data):
    """Mengubah data mentah menjadi format DataFrame untuk setiap dimensi."""
    print("Memulai proses transformasi dimensi...")
    transformed_dims = {}

    # 1. DimDate
    min_date = data['cust_order']['order_date'].min()
    max_date = data['cust_order']['order_date'].max()
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    dim_date = pd.DataFrame(data=date_range, columns=['full_date'])
    dim_date['date_sk'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['day_val'] = dim_date['full_date'].dt.day
    dim_date['month_val'] = dim_date['full_date'].dt.month
    dim_date['year_val'] = dim_date['full_date'].dt.year
    dim_date['quarter_val'] = dim_date['full_date'].dt.quarter
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    transformed_dims['dim_date'] = dim_date
    print("  - DimDate berhasil ditransformasi.")

    # 2. DimCustomer
    df_cust_link = pd.merge(data['customer'], data['customer_address'], on='customer_id')
    df_cust_addr = pd.merge(df_cust_link, data['address'], on='address_id')
    df_cust_country = pd.merge(df_cust_addr, data['country'], on='country_id')
    dim_customer_raw = pd.merge(df_cust_country, data['address_status'], on='status_id')
    dim_customer = dim_customer_raw[['customer_id', 'first_name', 'last_name', 'email', 'address_status', 'street_number', 'street_name', 'city', 'country_name']].copy()
    dim_customer.rename(columns={'country_name': 'country'}, inplace=True)
    transformed_dims['dim_customer'] = dim_customer
    print("  - DimCustomer berhasil ditransformasi.")

    # 3. DimShipping
    dim_shipping = data['shipping_method'][['method_id', 'method_name']].copy()
    dim_shipping.rename(columns={'method_id': 'shipping_method_id', 'method_name': 'shipping_method'}, inplace=True)
    transformed_dims['dim_shipping'] = dim_shipping
    print("  - DimShipping berhasil ditransformasi.")

    # 4. DimBook
    df_b_pub = pd.merge(data['book'], data['publisher'], on='publisher_id', how='left')
    df_b_pub_lang = pd.merge(df_b_pub, data['book_language'], on='language_id', how='left')
    df_authors = pd.merge(data['book_author'], data['author'], on='author_id').groupby('book_id')['author_name'].apply(', '.join).reset_index()
    dim_book_raw = pd.merge(df_b_pub_lang, df_authors, on='book_id', how='left')
    dim_book = dim_book_raw[['book_id', 'title', 'isbn13', 'language_code', 'language_name', 'num_pages', 'publication_date', 'publisher_name', 'author_name']].copy()
    transformed_dims['dim_book'] = dim_book
    print("  - DimBook berhasil ditransformasi.")
    
    total_temp_memory_bytes = 0
    for name, df in transformed_dims.items():
    	usage = df.memory_usage(deep=True).sum()
    	total_temp_memory_bytes += usage
    	print(f"  - Ukuran memori untuk {name}: {usage / 1024**2:.2f} MB")

    print(f"\nTotal Penggunaan Memori Sementara (RAM): {total_temp_memory_bytes / 1024**2:.2f} MB") 

    print("Proses transformasi dimensi selesai.")
    return transformed_dims

def load_dimensions_and_get_keys(dim_data):
    """Memuat dimensi ke DWH dan mengambil kembali surrogate keys."""
    print("Memulai proses pemuatan dimensi ke DWH...")
    keys = {}
    for name, df in dim_data.items():
        df.to_sql(name, dwh_engine, if_exists='append', index=False, chunksize=1000)
        print(f"  - Berhasil memuat data ke tabel: {name}")
        loaded_df = pd.read_sql_table(name, dwh_engine)
        
        if name == 'dim_book': keys[name] = loaded_df[['book_sk', 'book_id']]
        elif name == 'dim_customer': keys[name] = loaded_df[['customer_sk', 'customer_id']]
        elif name == 'dim_shipping': keys[name] = loaded_df[['shipping_sk', 'shipping_method_id']]
        elif name == 'dim_date': keys[name] = loaded_df[['date_sk', 'full_date']]

    print("Proses pemuatan dimensi selesai.")
    return keys

def transform_and_load_facts(source_data, dim_keys):
    """Membangun tabel fakta menggunakan surrogate keys dari dimensi."""
    print("Memulai proses transformasi dan pemuatan fakta...")
    
    # 1. Gabungkan order_line dan cust_order
    df_orders = pd.merge(source_data['order_line'], source_data['cust_order'], on='order_id')
    
    # --- PERBAIKAN DI SINI ---
    # 2. Gabungkan hasilnya dengan shipping_method DARI SUMBER untuk mendapatkan `cost`
    df_orders_with_cost = pd.merge(df_orders, source_data['shipping_method'], left_on='shipping_method_id', right_on='method_id', how='left')
    # Ganti nama kolom `cost` menjadi `shipping_cost` agar sesuai dengan tabel fakta
    df_orders_with_cost.rename(columns={'cost': 'shipping_cost'}, inplace=True)
    
    # 3. Gabungkan dengan kunci dimensi untuk mendapatkan SK
    fact_merged = pd.merge(df_orders_with_cost, dim_keys['dim_customer'], on='customer_id', how='left')
    fact_merged = pd.merge(fact_merged, dim_keys['dim_book'], on='book_id', how='left')
    fact_merged = pd.merge(fact_merged, dim_keys['dim_shipping'], on='shipping_method_id', how='left')
    
    fact_merged['order_date'] = pd.to_datetime(fact_merged['order_date']).dt.date
    dim_keys['dim_date']['full_date'] = pd.to_datetime(dim_keys['dim_date']['full_date']).dt.date
    fact_merged = pd.merge(fact_merged, dim_keys['dim_date'], left_on='order_date', right_on='full_date', how='left')

    # 4. Pilih kolom-kolom final untuk tabel fakta
    fact_book_sales = fact_merged[['customer_sk', 'book_sk', 'shipping_sk', 'date_sk', 'price', 'shipping_cost']].copy()
    
    fact_book_sales.dropna(inplace=True) 
    
    sk_columns = ['customer_sk', 'book_sk', 'shipping_sk', 'date_sk']
    fact_book_sales[sk_columns] = fact_book_sales[sk_columns].astype(int)

    print("  - FactBookSales berhasil ditransformasi.")

    fact_book_sales.to_sql('fact_book_sales', dwh_engine, if_exists='append', index=False, chunksize=1000)
    print("  - Berhasil memuat data ke tabel: fact_book_sales")
    print("Proses transformasi dan pemuatan fakta selesai.")


if __name__ == '__main__':
    start_time = time.time()
    
    prepare_dwh_tables()
    
    source_data = extract_data()
    if source_data:
        transformed_dims = transform_dimensions(source_data)
        dimension_keys = load_dimensions_and_get_keys(transformed_dims)
        transform_and_load_facts(source_data, dimension_keys)

    end_time = time.time()
    print(f"\nPipeline ETL DWH selesai dalam {end_time - start_time:.2f} detik.")

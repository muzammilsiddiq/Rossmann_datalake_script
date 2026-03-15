import pandas as pd
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Supabase configuration
# Load environment variables from .env file
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Update this path to where your CSV files are located
data_path = Path('data')


def load_raw_data():
    """Load raw data from CSV files"""
    logging.info("Loading data...")

    store_path = data_path / 'store.csv'
    train_path = data_path / 'train.csv'

    df_store = pd.read_csv(store_path)
    df_train = pd.read_csv(train_path, low_memory=False)

    logging.info(f"Store records: {len(df_store)}")
    logging.info(f"Training records: {len(df_train)}")

    return df_store, df_train


def create_dim_store(df_store):
    """Create store dimension"""
    dim = pd.DataFrame()
    dim['store_id'] = df_store['Store']
    dim['store_type'] = df_store['StoreType']

    # Map assortment codes to full names
    assort_map = {'a': 'Basic', 'b': 'Extra', 'c': 'Extended'}
    dim['assortment'] = df_store['Assortment'].map(assort_map)

    return dim.drop_duplicates().reset_index(drop=True)


def create_dim_competition(df_store):
    """Create competition dimension - NO ID column (auto-generated)"""
    comp_data = []
    for idx, row in df_store.iterrows():
        comp_data.append({
            'distance': int(row['CompetitionDistance']) if pd.notna(row['CompetitionDistance']) else 0,
            'open': 1 if pd.notna(row['CompetitionOpenSinceYear']) else 0,
            'open_year': int(row['CompetitionOpenSinceYear']) if pd.notna(row['CompetitionOpenSinceYear']) else 0,
            'open_month': int(row['CompetitionOpenSinceMonth']) if pd.notna(row['CompetitionOpenSinceMonth']) else 0
        })

    dim = pd.DataFrame(comp_data)
    return dim


def create_dim_promotion(df_store):
    """Create promotion dimension - NO ID column (auto-generated)"""
    promo_data = []
    for idx, row in df_store.iterrows():
        promo_data.append({
            'promotion': int(row['Promo2']) if pd.notna(row['Promo2']) else 0,
            'promotion_year': int(row['Promo2SinceYear']) if pd.notna(row['Promo2SinceYear']) else 0,
            'promotion_interval': str(row['PromoInterval']) if pd.notna(row['PromoInterval']) else ''
        })

    dim = pd.DataFrame(promo_data)
    return dim


def create_dim_time(df_train):
    """Create time dimension with holiday info - NO ID column (auto-generated)"""
    # Get unique dates and sort them
    unique_dates = pd.to_datetime(df_train['Date']).unique()
    unique_dates = sorted(unique_dates)

    time_data = []
    for date in unique_dates:
        # Get data for this date from training data
        day_data = df_train[pd.to_datetime(df_train['Date']) == date]

        # Determine holiday status
        school_holiday = day_data['SchoolHoliday'].mode()[0] if not day_data.empty else 0

        # Map state holiday: 0=None, a=public, b=Easter, c=Christmas
        state_holiday_map = {'0': 0, 'a': 1, 'b': 2, 'c': 3}
        state_holiday = day_data['StateHoliday'].map(state_holiday_map).mode()[0] if not day_data.empty else 0

        time_data.append({
            'full_date': date.strftime('%Y-%m-%d'),
            'school_holiday': int(school_holiday),
            'state_holiday': int(state_holiday),
            'month': date.month,
            'year': date.year
        })

    dim = pd.DataFrame(time_data)
    return dim


def get_dimension_ids():
    """Get IDs from dimension tables after upload"""
    # Get competition IDs with proper ordering
    comp_response = supabase.table('dim_competition').select('competition_id').order('competition_id').execute()
    competition_ids = [row['competition_id'] for row in comp_response.data]

    # Get promotion IDs with proper ordering
    promo_response = supabase.table('dim_promotion').select('promotion_id').order('promotion_id').execute()
    promotion_ids = [row['promotion_id'] for row in promo_response.data]

    # Get time IDs mapped by date
    time_response = supabase.table('dim_time').select('time_id, full_date').execute()
    time_map = {row['full_date']: row['time_id'] for row in time_response.data}

    return competition_ids, promotion_ids, time_map


def create_fact_sales(df_train, competition_ids, promotion_ids, time_map):
    """Create fact table - connects all dimensions"""
    df = df_train.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date_str'] = df['Date'].dt.strftime('%Y-%m-%d')

    # Create store to competition/promotion mapping
    # Since dimensions are in the same order as stores, we can use index
    store_to_comp = {i + 1: comp_id for i, comp_id in enumerate(competition_ids)}
    store_to_promo = {i + 1: promo_id for i, promo_id in enumerate(promotion_ids)}

    # Create fact table
    fact = pd.DataFrame()
    fact['store_id'] = df['Store']
    fact['dim_competition'] = df['Store'].map(store_to_comp)
    fact['dim_promotion'] = df['Store'].map(store_to_promo)
    fact['dim_time'] = df['Date_str'].map(time_map)
    fact['turnover'] = df['Sales']
    fact['nr_customers'] = df['Customers']

    # Calculate turnover per customer
    fact['turnover_per_customer'] = 0.0
    mask = fact['nr_customers'] > 0
    fact.loc[mask, 'turnover_per_customer'] = (fact.loc[mask, 'turnover'] / fact.loc[mask, 'nr_customers']).round(2)

    # Remove rows with missing mappings
    initial_count = len(fact)
    fact = fact.dropna(subset=['dim_competition', 'dim_promotion', 'dim_time'])

    # Convert to integers
    fact['dim_competition'] = fact['dim_competition'].astype(int)
    fact['dim_promotion'] = fact['dim_promotion'].astype(int)
    fact['dim_time'] = fact['dim_time'].astype(int)

    if len(fact) < initial_count:
        logging.warning(f"Dropped {initial_count - len(fact)} rows with missing foreign key mappings")

    return fact


def upload_to_supabase_batch(df, table_name, batch_size=500):
    """Upload dataframe to Supabase in batches"""
    total_rows = len(df)
    logging.info(f"Uploading {total_rows} rows to {table_name}...")

    # Convert dataframe to list of dictionaries
    records = df.to_dict('records')

    # Upload in batches
    for i in range(0, total_rows, batch_size):
        batch = records[i:i + batch_size]
        try:
            response = supabase.table(table_name).insert(batch).execute()
            logging.info(f"Uploaded batch {i // batch_size + 1}/{(total_rows - 1) // batch_size + 1} to {table_name}")
        except Exception as e:
            logging.error(f"Error uploading batch to {table_name}: {str(e)}")
            if batch:
                logging.error(f"First record in failed batch: {batch[0]}")
            raise e

    logging.info(f"Successfully uploaded all data to {table_name}")
    return response


def clear_tables():
    """Clear all tables before inserting new data"""
    tables = ['fact_sales', 'dim_time', 'dim_promotion', 'dim_competition', 'dim_store']

    for table in tables:
        try:
            supabase.table(table).delete().neq('store_id', -1).execute()
            logging.info(f"Cleared table: {table}")
        except Exception as e:
            logging.warning(f"Could not clear table {table}: {str(e)}")


def main():
    """Main ETL process"""
    try:
        # Load raw data
        df_store, df_train = load_raw_data()

        # Create dimensions (without ID columns)
        logging.info("Creating dimension tables...")
        dim_store = create_dim_store(df_store)
        dim_competition = create_dim_competition(df_store)
        dim_promotion = create_dim_promotion(df_store)
        dim_time = create_dim_time(df_train)

        logging.info(f"dim_store shape: {dim_store.shape}")
        logging.info(f"dim_competition shape: {dim_competition.shape}")
        logging.info(f"dim_promotion shape: {dim_promotion.shape}")
        logging.info(f"dim_time shape: {dim_time.shape}")

        # Clear existing data (uncomment if needed)
        # clear_tables()

        # Upload dimensions to Supabase
        logging.info("Uploading dimensions to Supabase...")
        upload_to_supabase_batch(dim_store, 'dim_store')
        upload_to_supabase_batch(dim_competition, 'dim_competition')
        upload_to_supabase_batch(dim_promotion, 'dim_promotion')
        upload_to_supabase_batch(dim_time, 'dim_time')

        # Get IDs from uploaded dimensions
        logging.info("Retrieving dimension IDs from database...")
        competition_ids, promotion_ids, time_map = get_dimension_ids()
        logging.info(f"Retrieved {len(competition_ids)} competition IDs")
        logging.info(f"Retrieved {len(promotion_ids)} promotion IDs")
        logging.info(f"Retrieved {len(time_map)} time IDs")

        # Create fact table
        logging.info("Creating fact table...")
        fact_sales = create_fact_sales(df_train, competition_ids, promotion_ids, time_map)
        logging.info(f"fact_sales shape: {fact_sales.shape}")

        # Upload fact table
        logging.info("Uploading fact table to Supabase...")
        upload_to_supabase_batch(fact_sales, 'fact_sales')

        logging.info("ETL process completed successfully!")

        # Print summary
        print("\n" + "=" * 50)
        print("DATA LOADING SUMMARY")
        print("=" * 50)
        print(f"dim_store: {len(dim_store)} records")
        print(f"dim_competition: {len(dim_competition)} records")
        print(f"dim_promotion: {len(dim_promotion)} records")
        print(f"dim_time: {len(dim_time)} records")
        print(f"fact_sales: {len(fact_sales)} records")
        print("=" * 50)

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        raise e


def verify_data():
    """Verify that data was loaded correctly"""
    try:
        tables = ['dim_store', 'dim_competition', 'dim_promotion', 'dim_time', 'fact_sales']

        print("\n" + "=" * 50)
        print("DATA VERIFICATION")
        print("=" * 50)

        for table in tables:
            response = supabase.table(table).select("*", count="exact").limit(5).execute()
            count = response.count if hasattr(response, 'count') else len(response.data)
            print(f"{table}: {count} records")

            if len(response.data) > 0:
                print(f"Sample from {table}:")
                for record in response.data:
                    print(f"  {record}")
            print()

    except Exception as e:
        logging.error(f"Verification failed: {str(e)}")


if __name__ == "__main__":
    # Run the ETL process
    main()

    # Verify the data was loaded
    verify_data()

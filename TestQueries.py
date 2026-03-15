# After loading data, you can run queries like this:
from IPython.core.magics import logging

from Main import supabase


def verify_data():
    """Verify that data was loaded correctly - without aggregates"""
    try:
        tables = ['dim_store', 'dim_competition', 'dim_promotion', 'dim_time', 'fact_sales']

        print("\n" + "=" * 50)
        print("DATA VERIFICATION")
        print("=" * 50)

        for table in tables:
            # Get count by fetching all and counting locally
            response = supabase.table(table).select("*").execute()
            count = len(response.data)
            print(f"{table}: {count} records")

            # Show sample from each table
            if count > 0:
                # Get a sample of 2 records
                sample_response = supabase.table(table).select("*").limit(2).execute()
                print(f"Sample from {table}:")
                for record in sample_response.data:
                    # Format the record for better readability
                    formatted_record = {k: v for k, v in record.items() if v is not None}
                    print(f"  {formatted_record}")
            print()

    except Exception as e:
        logging.error(f"Verification failed: {str(e)}")
        print(f"Error details: {e}")


def verify_fact_quality():
    """Verify fact table data quality without aggregates"""
    try:
        print("\n" + "=" * 50)
        print("FACT TABLE QUALITY CHECK")
        print("=" * 50)

        # Get all fact records
        response = supabase.table('fact_sales').select("*").execute()
        fact_data = response.data
        total_records = len(fact_data)
        print(f"Total fact records: {total_records}")

        if total_records == 0:
            print("No fact records found")
            return

        # Check for NULL foreign keys
        null_store = sum(1 for r in fact_data if r.get('store_id') is None)
        null_comp = sum(1 for r in fact_data if r.get('dim_competition') is None)
        null_promo = sum(1 for r in fact_data if r.get('dim_promotion') is None)
        null_time = sum(1 for r in fact_data if r.get('dim_time') is None)

        if null_store > 0:
            print(f"⚠ Found {null_store} records with NULL store_id")
        if null_comp > 0:
            print(f"⚠ Found {null_comp} records with NULL dim_competition")
        if null_promo > 0:
            print(f"⚠ Found {null_promo} records with NULL dim_promotion")
        if null_time > 0:
            print(f"⚠ Found {null_time} records with NULL dim_time")

        # Check for negative turnover
        negative_turnover = sum(1 for r in fact_data if r.get('turnover', 0) < 0)
        if negative_turnover > 0:
            print(f"⚠ Found {negative_turnover} records with negative turnover")

        # Get date range by fetching time dimension
        time_response = supabase.table('dim_time').select("full_date").order('full_date').execute()
        if time_response.data:
            dates = [t['full_date'] for t in time_response.data]
            print(f"Date range: {dates[0]} to {dates[-1]}")

        print("=" * 50)

    except Exception as e:
        logging.error(f"Fact quality check failed: {str(e)}")


def get_table_stats():
    """Get table statistics using multiple queries"""
    try:
        print("\n" + "=" * 50)
        print("TABLE STATISTICS")
        print("=" * 50)

        # Get all records and calculate stats locally
        tables = ['dim_store', 'dim_competition', 'dim_promotion', 'dim_time', 'fact_sales']

        for table in tables:
            response = supabase.table(table).select("*").execute()
            data = response.data
            print(f"{table}: {len(data)} records")

            # Show column info for first record
            if data:
                print(f"  Columns: {list(data[0].keys())}")
        print("=" * 50)

    except Exception as e:
        logging.error(f"Stats gathering failed: {str(e)}")


if __name__ == "__main__":
    verify_data()
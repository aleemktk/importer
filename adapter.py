#!/usr/bin/env python3
"""
Script to update sma_products table with discount columns from Excel file
This script:
1. Connects to MySQL database using SQLAlchemy
2. Adds discount columns if they don't exist
3. Updates products with discount values from Excel file
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sys
from typing import Optional

# Database configuration
DB_CONNECTION_STRING = "mysql+pymysql://root@localhost:3306/avnzor"
TABLE_NAME = "sma_products"
EXCEL_FILE_PATH = "discount.xls"

# Column definitions
# Note: cash_discount and credit_discount already exist in the database
NEW_DISCOUNT_COLUMNS = [
    "cash_dis2", 
    "cash_dis3",
    "credit_dis2",
    "credit_dis3"
]

ALL_DISCOUNT_COLUMNS = [
    "cash_discount",  # existing column (maps to cash_dis)
    "cash_dis2", 
    "cash_dis3",
    "credit_discount",  # existing column (maps to credit_dis)
    "credit_dis2",
    "credit_dis3"
]


def read_excel_file(file_path: str) -> pd.DataFrame:
    """
    Read and process the Excel file with discount data
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        DataFrame with processed discount data
    """
    print(f"Reading Excel file: {file_path}")
    
    # Read Excel file
    df = pd.read_excel(file_path)
    
    # The first row contains the actual headers
    # Set the first row as column names
    df.columns = df.iloc[0]
    
    # Drop the first row (now it's redundant as headers)
    df = df.drop(0).reset_index(drop=True)
    
    # Rename columns for clarity
    # Based on the structure: ITEM_NO, then CREDIT section (Dis1, Dis2, Dis3), then Cash section (Dis1, Dis2, Dis3)
    column_mapping = {
        'ITEM_NO': 'item_no',
        'Dis1': 'credit_dis',  # First Dis1 is under CREDIT
        'Dis2': 'credit_dis2',  # First Dis2 is under CREDIT
        'Dis3': 'credit_dis3'   # First Dis3 is under CREDIT
    }
    
    # Get Cash discount columns (they appear after CREDIT columns)
    cash_columns = df.columns.tolist()
    cash_dis_idx = [i for i, col in enumerate(cash_columns) if col == 'Dis1']
    
    if len(cash_dis_idx) >= 2:
        # Second occurrence of Dis1, Dis2, Dis3 are Cash discounts
        cash_start_idx = cash_dis_idx[1]
        df.columns.values[cash_start_idx] = 'cash_dis'
        df.columns.values[cash_start_idx + 1] = 'cash_dis2'
        df.columns.values[cash_start_idx + 2] = 'cash_dis3'
    
    df = df.rename(columns=column_mapping)
    
    # Select only the columns we need
    required_columns = ['item_no', 'credit_dis', 'credit_dis2', 'credit_dis3', 
                       'cash_dis', 'cash_dis2', 'cash_dis3']
    
    df = df[required_columns]
    
    # Rename to match database column names (cash_dis -> cash_discount, credit_dis -> credit_discount)
    df = df.rename(columns={
        'cash_dis': 'cash_discount',
        'credit_dis': 'credit_discount'
    })
    
    # Convert discount columns to numeric, replacing errors with None
    discount_cols = ['cash_discount', 'cash_dis2', 'cash_dis3', 
                     'credit_discount', 'credit_dis2', 'credit_dis3']
    for col in discount_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows where item_no is null or not numeric
    df = df[df['item_no'].notna()]
    df['item_no'] = pd.to_numeric(df['item_no'], errors='coerce')
    df = df[df['item_no'].notna()]
    
    print(f"Processed {len(df)} products from Excel file")
    print(f"\nSample data:")
    print(df.head())
    
    return df


def add_columns_if_not_exist(engine, table_name: str, columns: list):
    """
    Add columns to the table if they don't already exist
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        columns: List of column names to add
    """
    inspector = inspect(engine)
    existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
    
    with engine.connect() as conn:
        for column in columns:
            if column not in existing_columns:
                print(f"Adding column: {column}")
                sql = text(f"ALTER TABLE {table_name} ADD COLUMN {column} DECIMAL(10, 4) DEFAULT NULL")
                conn.execute(sql)
                conn.commit()
                print(f"✓ Column {column} added successfully")
            else:
                print(f"Column {column} already exists, skipping")


def update_products(engine, table_name: str, df: pd.DataFrame):
    """
    Update products with discount values from the DataFrame
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        df: DataFrame with discount data
    """
    print(f"\nUpdating {len(df)} products...")
    
    updated_count = 0
    error_count = 0
    
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            try:
                # Prepare the UPDATE query
                update_sql = text(f"""
                    UPDATE {table_name}
                    SET 
                        cash_discount = :cash_discount,
                        cash_dis2 = :cash_dis2,
                        cash_dis3 = :cash_dis3,
                        credit_discount = :credit_discount,
                        credit_dis2 = :credit_dis2,
                        credit_dis3 = :credit_dis3
                    WHERE id = :item_no
                """)
                
                # Execute the update
                result = conn.execute(update_sql, {
                    'item_no': int(row['item_no']),
                    'cash_discount': float(row['cash_discount']) if pd.notna(row['cash_discount']) else None,
                    'cash_dis2': float(row['cash_dis2']) if pd.notna(row['cash_dis2']) else None,
                    'cash_dis3': float(row['cash_dis3']) if pd.notna(row['cash_dis3']) else None,
                    'credit_discount': float(row['credit_discount']) if pd.notna(row['credit_discount']) else None,
                    'credit_dis2': float(row['credit_dis2']) if pd.notna(row['credit_dis2']) else None,
                    'credit_dis3': float(row['credit_dis3']) if pd.notna(row['credit_dis3']) else None,
                })
                
                if result.rowcount > 0:
                    updated_count += 1
                    if (idx + 1) % 100 == 0:
                        print(f"Progress: {idx + 1}/{len(df)} products processed...")
                        
            except Exception as e:
                error_count += 1
                print(f"Error updating product {row['item_no']}: {str(e)}")
        
        # Commit all changes
        conn.commit()
    
    print(f"\n✓ Update completed!")
    print(f"  - Successfully updated: {updated_count} products")
    print(f"  - Errors: {error_count}")


def main():
    """Main execution function"""
    try:
        print("=" * 70)
        print("DISCOUNT UPDATE SCRIPT")
        print("=" * 70)
        
        # Create database engine
        print(f"\nConnecting to database: {DB_CONNECTION_STRING}")
        engine = create_engine(DB_CONNECTION_STRING, echo=False)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Database connection successful")
        
        # Add only the new columns (cash_discount and credit_discount already exist)
        print(f"\nChecking/adding new discount columns to {TABLE_NAME}...")
        print("Note: cash_discount and credit_discount already exist in the database")
        add_columns_if_not_exist(engine, TABLE_NAME, NEW_DISCOUNT_COLUMNS)
        
        # Read and process Excel file
        df = read_excel_file(EXCEL_FILE_PATH)
        
        # Update products
        update_products(engine, TABLE_NAME, df)
        
        print("\n" + "=" * 70)
        print("SCRIPT COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
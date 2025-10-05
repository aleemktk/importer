from database import SessionLocal
from utils import read_excel_file, split_dataframe_in_batches
from services.product_service import get_existing_product_codes, insert_missing_products
from services.purchase_service import create_purchase
from services.report_service import generate_import_report

EXCEL_FILE = "abaad_files/pharmacyno_1.xlsx"
BATCH_SIZE = 1000

def process_excel_batches(file_path: str):
    df = read_excel_file(file_path, header=None)

    df = df.iloc[1:].reset_index(drop=True)

    df.columns = [
        "item_code",     # index 0
        "item_name",          # index 1
        "item_batch_number",   # index 2
        "item_ascon_code", # index 3
        "item_expiry_date",    # index 
        "item_quantity",  # index 4
        "item_sale_price",
        "item_total_sale_price",
        "item_purchase_price",
        "item_total_purchase_price", 
        "item_cost_price", 
        "item_total_cost_price", # "item_before_vat", same index 
        "vat_value", 
        "item_total_vat", 
        "item_total_after_vat"

        # Add more columns if needed
    ]

                     
    batches = split_dataframe_in_batches(df, BATCH_SIZE)

    for batch_df in batches:
        session = SessionLocal()
        try:

            product_codes = batch_df["item_code"].unique().tolist()
            existing_codes = get_existing_product_codes(session, product_codes)
            # print(f"Existing product codes: {existing_codes}")
            # Filter missing products
            missing_products = batch_df[~batch_df["item_code"].isin(existing_codes)]
            products_to_insert = missing_products.apply(lambda row: {
                "name": row["item_name"],
                "item_code": row["item_code"],
                "category_id": row.get("item_code", 3),
                "cost_price": row["item_cost_price"],
                "sale_price": row["item_sale_price"],
            }, axis=1).tolist()

            insert_missing_products(session, products_to_insert)

            # Insert purchases and items

            create_purchase(session, batch_df)
        except Exception as e:
            print(f"Error processing batch: {e}")
            session.rollback()
        finally:
            session.close()
    # generate_import_report()        

if __name__ == "__main__":
    process_excel_batches(EXCEL_FILE)
    print("Processing complete.")

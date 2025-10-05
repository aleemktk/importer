
import pandas as pd

from database import SessionLocal
from utils import read_excel_file, split_dataframe_in_batches, generate_excel_report
from services.product_service import get_existing_product_codes, insert_missing_products
from services.purchase_service import create_purchase
from services.report_service import generate_import_report
from services.supplier_service import get_existing_suppliers, insert_missing_suppliers
from services.category_service import get_existing_categories, insert_missing_categories
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from model import Category

def process_file(task_id: str, file_path: str):
    try:
        created_purchase_ids = []
        created_transfer_ids = []
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        log_step(task_id, "Step 1: Reading Excel file...")
        df = pd.read_excel(file_path, header=None)
        df = df.iloc[1:].reset_index(drop=True)

        # df.columns = [
        #     "item_code", "item_name", "item_batch_number", "item_ascon_code", "item_expiry_date",
        #     "item_quantity", "item_sale_price", "item_total_sale_price",
        #     "item_purchase_price", "item_total_purchase_price",
        #     "item_cost_price", "item_total_cost_price",
        #     "vat_value", "item_total_vat", "item_total_after_vat"
        # ]
        # ProductId	Product	StockId	PackUnits	Packs	Units	SalePrice	CostPrice	DealCost	TotalSale	TotalCost	TotalDealCost	BatchNo	Expiry	Branch	Store	Supplier	Category	Group	VAT
        # ProductId	ProductEn	ProductAr	Barcode	StockId	PackUnits	Packs	Units	SalePrice	CostPrice	TotalSale	TotalCost	BatchNo	Expiry	Branch	Store	Supplier	Category

        df.columns = [
            "product_id", "item_name",
            "product_arabic_name", "item_code", "stock_id", "item_packs_units", "item_quantity", "item_units",
            "item_sale_price", 
            "item_cost_price",
            "item_total_sale_price", 
            "item_total_cost_price",
            "item_batch_number",
            "item_expiry_date", "branch", "store", "supplier",
            "category"
        ]

        df['item_total_vat'] = 0 
        df['item_total_after_vat'] = 0
        df['total_sale_vat'] = 0
        df['total_sale'] = 0


        #df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
        #df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]

        log_step(task_id, "Step 2: Splitting file into batches...")
        batches = split_dataframe_in_batches(df, BATCH_SIZE)

        for i, batch_df in enumerate(batches):
            session = SessionLocal()
            try:
                log_step(task_id, f"➡️ Processing batch {i + 1}...")

                 # Step 1: Add suppliers if not exist
                log_step(task_id, "➡️ Checking suppliers...")

                suppliers_in_batch = batch_df["supplier"].dropna().unique().tolist()
                existing_suppliers = get_existing_suppliers(session, suppliers_in_batch)

                missing_suppliers = [s for s in suppliers_in_batch if s not in existing_suppliers]

                if missing_suppliers:
                    log_step(task_id, f"➡️ Inserting {len(missing_suppliers)} new suppliers...")
                    suppliers_to_insert = [{"name": s} for s in missing_suppliers]
                    insert_missing_suppliers(session, suppliers_to_insert)
                else:
                    log_step(task_id, "✅ No new suppliers to add.")

                 # Step 2: Add categories if not exist
                log_step(task_id, "➡️ Checking categories...")

                categories_in_batch = batch_df["category"].dropna().unique().tolist()
                existing_categories = get_existing_categories(session, categories_in_batch)

                missing_categories = [c for c in categories_in_batch if c not in existing_categories]

                if missing_categories:
                    log_step(task_id, f"➡️ Inserting {len(missing_categories)} new categories...")
                    categories_to_insert = [{"name": c} for c in missing_categories]
                    insert_missing_categories(session, categories_to_insert)
                else:
                    log_step(task_id, "✅ No new categories to add.")    

                category_map = dict(
                    session.query(Category.name, Category.id)
                    .filter(Category.name.in_(categories_in_batch))
                    .all()
                )    

                product_codes = batch_df["barcode"].unique().tolist()
                existing_codes = get_existing_product_codes(session, product_codes)

                log_step(task_id, f"➡️ Checking missing product ...")

                missing_products = batch_df[~batch_df["barcode"].isin(existing_codes)]

                products_to_insert = missing_products.apply(lambda row: {
                    "name": row["product_name"],
                    "item_code": row["barcode"],
                    "code" : row["barcode"],
                    "category_id": category_map.get(row["category"]),
                    "cost_price": row["item_cost_price"],
                    "sale_price": row["item_sale_price"],
                }, axis=1).tolist()

                log_step(task_id, f"➡️ Insert missing products...")


                insert_missing_products(session, products_to_insert)

                log_step(task_id, f"➡️ Create Purchase {i + 1}...")

                result = create_purchase(session, batch_df)
                if result.get("purchase_id"):
                    created_purchase_ids.append(result["purchase_id"])
    
                # if result.get("transfer_id"):
                #     created_transfer_ids.append(result["transfer_id"])


                log_step(task_id, f"✅ Batch {i + 1} inserted successfully.")
            except Exception as e:
                session.rollback()
                log_step(task_id, f"❌ Error in batch {i + 1}: {str(e)}")
            finally:
                session.close()

        log_step(task_id, "Step 3: All batches processed successfully.")
        log_step(task_id, "Step 4: Generating report...")

        # Save dummy report (you'll replace this logic later)
        # report_path = f"reports/{task_id}_report.xlsx"
        # os.makedirs("reports", exist_ok=True)
        # with open(report_path, "w") as f:
        #     f.write("Dummy Excel content")
        generate_excel_report(task_id, session, purchase_ids=created_purchase_ids, transfer_ids=created_transfer_ids)

        end_time = datetime.datetime.now()
        log_step(task_id, f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")    

        total_duration = end_time - start_time
        log_step(task_id, f"⏱️ Total Duration: {total_duration}")

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["report_url"] = f"/download/{task_id}"
        log_step(task_id, "✅ Import completed successfully.")

    except Exception as e:
        tasks[task_id]["status"] = "failed"
        log_step(task_id, f"❌ Error: {str(e)}")
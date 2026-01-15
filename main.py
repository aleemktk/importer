from fastapi import FastAPI, File, UploadFile, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from uuid import uuid4
import pandas as pd
import shutil
import os
from database import SessionLocal
from utils import read_excel_file, split_dataframe_in_batches, generate_excel_report
from services.product_service import get_existing_product_codes, insert_missing_products
from services.purchase_service import create_purchase
from services.purchase_rawabi_service import create_rawabi_purchase
from services.report_service import generate_import_report
from services.image_service import update_product_image, check_product_exists
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from model import Category, Supplier, Product
from services.supplier_service import get_existing_suppliers, insert_missing_suppliers
from services.category_service import get_existing_categories, insert_missing_categories
from services.jarir.purchase_service import create_purchase as jarir_create_purchase
from sqlalchemy import tuple_
import sys
from openpyxl_image_loader import SheetImageLoader
from openpyxl import load_workbook
import requests
from sqlalchemy import text

import datetime
app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

EXCEL_FILE = "abaad_files/pharmacyno_1.xlsx"
BATCH_SIZE = 1000

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("temp", exist_ok=True)

IMAGE_UPLOAD_DIR = "images/products"
os.makedirs(IMAGE_UPLOAD_DIR, exist_ok=True)

tasks = {}



@app.post("/upload")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting import..."],
        "report_url": None
    }

    background_tasks.add_task(process_file, task_id, file_location)
    return {"task_id": task_id}

@app.post("/upload_rawabi_products")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting import..."],
        "report_url": None
    }

    background_tasks.add_task(rawabi_products_process_file, task_id, file_location)
    return {"task_id": task_id}

@app.post("/rawabi_inventory_file")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting import..."],
        "report_url": None
    }

    background_tasks.add_task(rawabi_inventory_process_file, task_id, file_location)
    return {"task_id": task_id}

@app.post("/upload_jarir")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting import..."],
        "report_url": None
    } 

    background_tasks.add_task(jarir_process_file, task_id, file_location)
    return {"task_id": task_id}

@app.post("/upload_jarir_metadata")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting import..."],
        "report_url": None
    }

    background_tasks.add_task(upload_jarir_metadata, task_id, file_location)
    return {"task_id": task_id}

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    return tasks.get(task_id, {"status": "not_found", "logs": []})

def log_step(task_id, message):
    tasks[task_id]["logs"].append(message)

def log_product_comparison(task_id, session, item_code, excel_row):
    """
    Compare and log differences between Excel data and database for existing products.
    
    Args:
        task_id: Task ID for logging
        session: Database session
        item_code: Product code to check
        excel_row: Row from Excel file containing product data
    """
    db_product = session.query(Product).filter(Product.code == item_code).first()
    
    if db_product:
        log_step(task_id, f"📋 Product {item_code} already exists - Comparison:")
        log_step(task_id, f"   Excel: name='{excel_row['item_name']}', cost_price={excel_row['item_cost_price']}")
        log_step(task_id, f"   DB: name='{db_product.name}', cost={db_product.cost}")

def extract_and_save_image(item_code, row_number):
    """
    Extract image from Excel cell and save it to disk.
    Returns image path or None.
    """
    try:
        cell_ref = f"B{row_number}"  # Image column
        if image_loader.image_in(cell_ref):
            img = image_loader.get(cell_ref)
            image_path = f"{IMAGE_DIR}/{item_code}.png"
            img.save(image_path)
            return image_path
    except Exception as e:
        print(f"⚠️ Image error for {item_code}: {e}")

    return None


## COMMERCE MASTER DATA
def rawabi_products_process_file(task_id: str, file_path: str):
    try:
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Step 1: Read Excel
        log_step(task_id, "Step 1: Reading Excel file...")
        df = pd.read_excel(file_path, header=None)
        df = df.iloc[1:].reset_index(drop=True)  # skip header row manually
        df = df.dropna(how='all')

        # Assign expected column names (position-based)
        # df.columns = [
        #     "item_code", 
        #     "item_name_ar",
        #     "item_name_en", 
        #     "item_vat"
        # ]

        # df = df.fillna({
        #     "item_code": "",
        #     "item_name_ar": "",
        #     "item_name_en": "",
        #     "item_vat": 0
        # })

        df.columns = [
            "item_code", "item_name", "item_batch_number",  "item_expiry_date",
            "item_quantity", "item_sale_price",  "item_purchase_price", "item_cost_price",  "vat_value",  
            "supplier_id", "supplier_name", "image"
        ]

        # Replace NaN values with 0 for numeric columns to avoid MySQL errors
        df["item_cost_price"] = df["item_cost_price"].fillna(0)
        df["item_sale_price"] = df["item_sale_price"].fillna(0)

        df = df.drop_duplicates(subset=['item_code'], keep='first').reset_index(drop=True)

        log_step(task_id, f"✅ Removed duplicates. {len(df)} unique items remaining.")

        log_step(task_id, f"📄 Loaded {len(df)} rows from file.")


        # Step 2: Split into batches
        log_step(task_id, "Step 2: Splitting file into batches...")
        batches = split_dataframe_in_batches(df, BATCH_SIZE)

        # Step 3: Process each batch
        for i, batch_df in enumerate(batches):
            session = SessionLocal()
            try:
                log_step(task_id, f"➡️ Processing batch {i + 1}/{len(batches)}...")

                # 1. Get unique codes from this batch (clean up NaNs)
                batch_codes = batch_df['item_code'].dropna().unique().tolist()
                
                # 2. Query DB once to find which of these codes ALREADY exist
                existing_codes = session.query(Product.code).filter(Product.code.in_(batch_codes)).all()
                existing_codes_set = {c[0] for c in existing_codes}

                records = []
                seen_in_this_batch = set() # To prevent duplicates if the same code repeats in this batch

                for index, row in batch_df.iterrows():
                    item_code = str(int(row['item_code'])) if pd.notna(row['item_code']) else None
                    
                    # Skip if NaN, already in DB, or already processed in this specific batch loop
                    if item_code is None or item_code in seen_in_this_batch:
                        continue
                    
                    
                    # Check if already exists in DB and log comparison
                    if item_code in existing_codes_set:
                        log_product_comparison(task_id, session, item_code, row)
                        continue  

                    # Add to the insert list
                    records.append({
                        "name_ar": row["item_name"],
                        "name": row["item_name"], 
                        "code": item_code,
                        "cost": row["item_cost_price"],
                        "price" : row["item_sale_price"],
                        #"category_id" : row['category'],
                        "image" : row['image'],
                        "tax_rate": 5 if row["vat_value"] == '15' else row["vat_value"]
                    })
                    
                    # Mark as seen so if it appears again in the same batch, it's skipped
                    seen_in_this_batch.add(item_code)

                # 3. Bulk insert only the truly new records
                if records:
                    session.bulk_insert_mappings(Product, records)
                    session.commit()
                    log_step(task_id, f"✅ Batch {i + 1} done: {len(records)} new items added.")
                else:
                    log_step(task_id, f"ℹ️ Batch {i + 1}: No new records to insert.")

            except Exception as e:
                session.rollback()
                log_step(task_id, f"❌ Error in batch {i + 1}: {str(e)}")
            finally:
                session.close()

        # Step 4: Final summary
        end_time = datetime.datetime.now()
        total_duration = end_time - start_time

        log_step(task_id, "Step 3: All batches processed successfully.")
        log_step(task_id, f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        log_step(task_id, f"⏱️ Total Duration: {total_duration}")

        tasks[task_id]["status"] = "completed"
        log_step(task_id, "✅ Import completed successfully.")

    except Exception as e:
        tasks[task_id]["status"] = "failed"
        log_step(task_id, f"❌ Fatal Error: {str(e)}")


def rawabi_inventory_process_file_old(task_id: str, file_path: str):
    try:
        created_purchase_ids = []
        created_transfer_ids = []
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        log_step(task_id, "Step 1: Reading Excel file...")
        df = pd.read_excel(file_path, header=None)
        df = df.iloc[1:].reset_index(drop=True)
        df = df.dropna(how='all')

        # df.columns = [
        #     "item_code", "item_name", "item_batch_number",  "item_expiry_date",
        #     "item_quantity", "item_sale_price", "item_total_sale_price",
        #     "item_purchase_price", "item_total_purchase_price",
        #     "item_cost_price", "item_total_cost_price",
        #     "vat_value", "item_total_vat", "item_total_after_vat", "supplier_id", "supplier_name","item_discount"
        # ]

        df.columns = [
            "item_code", "item_name", "item_batch_number",  "item_expiry_date",
            "item_quantity",  "item_purchase_price", "vat_value",  "item_cost_price","item_sale_price", 
            "supplier_id", "supplier_name"
        ]
   

        df["item_total_sale_price"] = df["item_sale_price"] * df["item_quantity"]
        df["item_total_purchase_price"] = df["item_purchase_price"] * df["item_quantity"]
        df["item_total_cost_price"] = df["item_cost_price"] * df["item_quantity"]
        df["item_total_vat"] = (df["item_cost_price"] * df["vat_value"]) / 100
        df["item_total_after_vat"] = df["item_total_cost_price"] + df["item_total_vat"]

        df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
        df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]
        df["item_batch_number"] = df["item_batch_number"].fillna('AAA')
        df["item_name"] = df["item_name"].fillna('empty product')
        df["item_discount"] = 0
        
        print(df.head(5))
        sys.exit
        
          # Step 2: Group data by supplier
        log_step(task_id, "Step 2: Grouping inventory by supplier_id...")
        grouped = df.groupby("supplier_id")

        # Step 3: Process each supplier batch
        for supplier_id, supplier_df in grouped:
            session = SessionLocal()
            try:
                log_step(task_id, f"➡️ Processing supplier_id {supplier_id} with {len(supplier_df)} items...")

                result = create_rawabi_purchase(session, supplier_df)
                if result.get("purchase_id"):
                    created_purchase_ids.append(result["purchase_id"])


                log_step(task_id, f"✅ Purchase created for supplier {supplier_id} with {len(supplier_df)} items.")

            except Exception as e:
                session.rollback()
                log_step(task_id, f"❌ Error processing supplier {supplier_id}: {str(e)}")

            finally:
                session.close()

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



def rawabi_inventory_process_file(task_id: str, file_path: str):
    try:
        created_purchase_ids = []
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # --- Step 1: Data Preparation ---
        df = pd.read_excel(file_path, header=None)
        df = df.iloc[1:].reset_index(drop=True)
        
        # df.columns = [
        #     "item_code", "item_name", "item_batch_number", "item_expiry_date",
        #     "item_quantity", "item_sale_price", "item_purchase_price", "item_cost_price",  "vat_value", 
        #      "supplier_id", "supplier_name","brand"
        # ]
        df.columns = [
            "item_code", "item_name", "item_batch_number",  "item_expiry_date",
            "item_quantity", "item_sale_price",  "item_purchase_price", "item_cost_price",  "vat_value",  
            "supplier_id", "supplier_name", "image"
        ]

        # Critical Validation: Remove rows with null item_code
        initial_count = len(df)
        df = df.dropna(subset=['item_code'])
        if len(df) < initial_count:
            log_step(task_id, f"⚠️ Dropped {initial_count - len(df)} rows due to missing item_code.")

        # Vectorized Calculations (Faster than loops)
        df["item_total_sale_price"] = df["item_sale_price"] * df["item_quantity"]
        df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
        df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]
        
        df["item_total_cost_price"] = df["item_cost_price"] * df["item_quantity"]
        df["item_total_vat"] = (df["item_total_cost_price"] * df["vat_value"]) / 100
        df["item_total_after_vat"] = df["item_total_cost_price"] + df["item_total_vat"]
        df["item_batch_number"] = df["item_batch_number"].fillna('AAA')
        df["item_name"] = df["item_name"].fillna('empty product')

        # --- Step 2: Ensure Products Exist (The Runtime Check) ---
        sync_products_in_db(task_id, df)

        # --- Step 3: Group by Supplier & Create Orders ---
        grouped = df.groupby("supplier_id")
        for supplier_id, supplier_df in grouped:
            session = SessionLocal()
            try:
                # Pass the cleaned dataframe to your purchase creation logic
                result = create_rawabi_purchase(session, supplier_df)
                if result.get("purchase_id"):
                    created_purchase_ids.append(result["purchase_id"])
                
                session.commit()
                log_step(task_id, f"✅ Purchase created for supplier {supplier_id}")
            except Exception as e:
                session.rollback()
                log_step(task_id, f"❌ Error for supplier {supplier_id}: {str(e)}")
            finally:
                session.close()

        # Finalize
        generate_excel_report(task_id, created_purchase_ids) # Pass IDs, let function open own session
        
        tasks[task_id].update({"status": "completed", "report_url": f"/download/{task_id}"})
        log_step(task_id, "✅ Import completed successfully.")

    except Exception as e:
        tasks[task_id]["status"] = "failed"
        log_step(task_id, f"❌ Critical Error: {str(e)}")

def sync_products_in_db(task_id, df):
    """Checks all codes in DF, inserts missing ones into the products table."""
    session = SessionLocal()
    try:
        unique_codes = df['item_code'].unique().tolist()
        
        # Find which codes already exist
        existing_codes = session.query(Product.code).filter(Product.code.in_(unique_codes)).all()
        existing_codes_set = {c[0] for c in existing_codes}

        new_products = []
        seen_in_df = set()

        for _, row in df.iterrows():
            code = str(row['item_code'])
            if code not in existing_codes_set and code not in seen_in_df:
                new_products.append({
                    "code": code,
                    "name": row["item_name"],
                    "name_ar": row["item_name"],
                    "cost": row["item_cost_price"],
                    "price": row["item_sale_price"],
                    "tax_rate": row["vat_value"]
                })
                seen_in_df.add(code)

        if new_products:
            session.bulk_insert_mappings(Product, new_products)
            session.commit()
            log_step(task_id, f"🆕 Registered {len(new_products)} new products in database.")
    finally:
        session.close()



def process_file(task_id: str, file_path: str):
    try:
        created_purchase_ids = []
        created_transfer_ids = []
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        log_step(task_id, "Step 1: Reading Excel file...")
        df = pd.read_excel(file_path, header=None)
        df = df.iloc[1:].reset_index(drop=True)

        df.columns = [
            "item_code", "item_name", "item_batch_number", "item_ascon_code", "item_expiry_date",
            "item_quantity", "item_sale_price", "item_total_sale_price",
            "item_purchase_price", "item_total_purchase_price",
            "item_cost_price", "item_total_cost_price",
            "vat_value", "item_total_vat", "item_total_after_vat"
        ]

        df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
        df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]

        log_step(task_id, "Step 2: Splitting file into batches...")
        batches = split_dataframe_in_batches(df, BATCH_SIZE)

        for i, batch_df in enumerate(batches):
            session = SessionLocal()
            try:
                log_step(task_id, f"➡️ Processing batch {i + 1}...")

                product_codes = batch_df["item_code"].unique().tolist()
                print(product_codes)
                existing_codes = get_existing_product_codes(session, product_codes)

                log_step(task_id, f"➡️ Checking missing product ...")

                missing_products = batch_df[~batch_df["item_code"].isin(existing_codes)]
                print(missing_products)

                # products_to_insert = missing_products.apply(lambda row: {
                #     "name": row["item_name"],
                #     "item_code": row["item_code"],
                #     "category_id": row.get("item_code", 3),
                #     "cost_price": row["item_cost_price"],
                #     "sale_price": row["item_sale_price"],
                #     "tax_rate" : 1
                # }, axis=1).tolist()

                # log_step(task_id, f"➡️ Insert missing products...")

                # insert_missing_products(session, products_to_insert)

                # Process missing products row by row
                for _, row in missing_products.iterrows():
                    existing_product = session.query(Product).filter_by(code=row["item_code"]).first()
                    if existing_product:
                        # Update existing product
                        existing_product.name = row["item_name"]
                        existing_product.item_code = row["item_code"]
                        existing_product.category_id = row.get("item_code", 3)
                        existing_product.cost = row["item_cost_price"]
                        existing_product.price = row["item_sale_price"]
                        log_step(task_id, f"🔄 Updated product {row['item_code']}") 
                    else:
                        # Insert new product
                        new_product = Product(
                            name=row["item_name"],
                            item_code=row["item_code"],
                            code=row["item_code"],
                            category_id=row.get("item_code", 3),
                            cost=row["item_cost_price"],
                            price=row["item_sale_price"],
                            tax_rate=1
                        )
                        session.add(new_product)
                        log_step(task_id, f"➕ Inserted product {row['item_code']}")

                session.commit()
                log_step(task_id, f"✅ Batch {i + 1} products inserted/updated successfully.")

                log_step(task_id, f"➡️ Fetching product VAT info and create batch")

                # Step 2: query products with their VAT rate from DB
                products = session.query(Product.item_code, Product.tax_rate).filter(
                    Product.item_code.in_(product_codes)
                ).all()

                  # Convert to dict: {item_code: vat_rate}
                #vat_map = {p.item_code: p.tax_rate for p in products}
                # def calc_vat(row):
                #     tax_rate = vat_map.get(row["item_code"], 0) or 0
                #     # business rule: if tax_rate == 5 → VAT = 15%, else 0
                #     vat_rate = 0.15 if tax_rate == 5 else 1
                #     vat_value = row["item_total_cost_price"] * vat_rate
                #     total_after_vat = row["item_total_cost_price"] + vat_value

                #      # df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
                #      # df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]
                #     total_sale_vat = row["item_total_sale_price"] * vat_rate
                #     total_sale = row["item_total_sale_price"] + total_sale_vat

                #     return pd.Series({
                #         "vat_value": 15 if tax_rate == 5 else 0,
                #         "item_total_vat": vat_value,  # same as vat_value per row
                #         "item_total_after_vat": total_after_vat,
                #         "total_sale_vat" : total_sale_vat,
                #         "total_sale" : total_sale
                #     })

                # batch_df[["vat_value", "item_total_vat", "item_total_after_vat", "total_sale_vat","total_sale"]] = batch_df.apply(calc_vat, axis=1)


                log_step(task_id, f"➡️ Create Purchase and Make transfer {i + 1}...")

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



def process_images_file(task_id: str, file_path: str):
    """
    Process Excel file containing product_code and image_url columns.
    Updates image_url_new column in sma_products table for matching products.
    """
    try:
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Read Excel file
        log_step(task_id, "Step 1: Reading Excel file...")
        
        # Try to read as CSV first, then fall back to Excel
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            # If extension doesn't match content, try the other format
            try:
                df = pd.read_csv(file_path)
            except:
                df = pd.read_excel(file_path, engine='openpyxl')
        
        # Check if required columns exist
        if 'product_code' not in df.columns or 'image_url' not in df.columns:
            log_step(task_id, "❌ Error: Excel file must contain 'product_code' and 'image_url' columns")
            tasks[task_id]["status"] = "failed"
            return
        
        # Remove rows with missing values
        df = df.dropna(subset=['product_code', 'image_url'])
        log_step(task_id, f"📄 Loaded {len(df)} rows from file.")
        
        # Step 2: Process each row
        log_step(task_id, "Step 2: Processing image updates...")
        
        session = SessionLocal()
        updated_count = 0
        not_found_count = 0
        error_count = 0
        
        try:
            for idx, row in df.iterrows():
                product_code = str(row['product_code']).strip()
                image_url = str(row['image_url']).strip()
                
                try:
                    # Update product image
                    if update_product_image(session, product_code, image_url):
                        updated_count += 1
                        if (idx + 1) % 100 == 0:  # Log progress every 100 rows
                            log_step(task_id, f"   Processed {idx + 1}/{len(df)} rows...")
                    else:
                        not_found_count += 1
                        log_step(task_id, f"⚠️ Product code '{product_code}' not found in database")
                        
                except Exception as e:
                    error_count += 1
                    log_step(task_id, f"❌ Error updating product '{product_code}': {str(e)}")
            
            # Final summary
            log_step(task_id, f"")
            log_step(task_id, f"📊 Summary:")
            log_step(task_id, f"   ✅ Successfully updated: {updated_count}")
            log_step(task_id, f"   ⚠️ Products not found: {not_found_count}")
            log_step(task_id, f"   ❌ Errors: {error_count}")
            
        finally:
            session.close()
        
        end_time = datetime.datetime.now()
        log_step(task_id, f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_duration = end_time - start_time
        log_step(task_id, f"⏱️ Total Duration: {total_duration}")
        
        tasks[task_id]["status"] = "completed"
        log_step(task_id, "✅ Image update completed successfully.")
        
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        log_step(task_id, f"❌ Error: {str(e)}")
    finally:
        # Clean up the uploaded file
        if os.path.exists(file_path):
            os.remove(file_path)

def update_product_barcode(session, product_code, data):
    product = session.query(Product).filter_by(code=product_code).first()
    print(data)
    # error updating product '748927068351': 'str' object has no attribute 'get'
    if product:
         product_data = data.get('product', {})                     
         product_name = product_data.get('name')
         image_url = product_data.get('imageUrl')
         product_upc = product_data.get('upc')
         product_ean = product_data.get('ean')
         description = product_data.get('description')
         product_barcode_url = data.get('barcodeUrl')

         query = text("""
                    UPDATE sma_products 
                    SET product_image = :image_url ,
                       product_name_external = :product_name,
                       upc = :product_upc,
                       ean = :product_ean,
                      product_details = :description,
                      external_barcode_url =:product_barcode_url
                    WHERE code = :product_code
                """)
                
         result = session.execute(query, {
                    "image_url": image_url,
                    "product_name" : product_name,
                    "product_upc" : product_upc,
                    "product_ean" : product_ean,
                    "description" : description,
                    "product_barcode_url" : product_barcode_url,
                    "product_code": product_code,

                })
         session.commit()
         return True


def process_external_images(task_id: str, file_path: str):
    """
    Process Excel file containing product_code and image_url columns.
    Updates image_url_new column in sma_products table for matching products.
    """
    try:
        start_time = datetime.datetime.now()
        log_step(task_id, f"📅 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Read Excel file
        log_step(task_id, "Step 1: Reading Excel file...")
        
        # Try to read as CSV first, then fall back to Excel
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            # If extension doesn't match content, try the other format
            try:
                df = pd.read_csv(file_path)
            except:
                df = pd.read_excel(file_path, engine='openpyxl')
        
        # Check if required columns exist
        if 'product_code' not in df.columns:
            log_step(task_id, "❌ Error: Excel file must contain 'product_code' columns")
            tasks[task_id]["status"] = "failed"
            return
        
        # Remove rows with missing values
        df = df.dropna(subset=['product_code'])
        log_step(task_id, f"📄 Loaded {len(df)} rows from file.")
        
        # Step 2: Process each row
        log_step(task_id, "Step 2: Processing image extracting...")
        
        session = SessionLocal()
        updated_count = 0
        not_found_count = 0
        error_count = 0
        
        try:
            for idx, row in df.iterrows():
                product_code = str(row['product_code']).strip()
                API_BASE_URL = 'https://go-upc.com/api/v1/code/'
                API_KEY = '20d20032dee9b95ff500dd1d47470e110391c9d12b3e0c50b93fd9c76b7a69a9'
                
                try:
                    # Update product image
                    # https://go-upc.com/api/v1/code/748927068801?key=20d20032dee9b95ff500dd1d47470e110391c9d12b3e0c50b93fd9c76b7a69a9
                    api_url = f"{API_BASE_URL}{product_code}?key={API_KEY}"
                    response = requests.get(api_url)
                    if response.status_code == 200:
                        print(response)
                        data = response.json()
                        
                    
                        # 2. Update Database
                        # Assuming update_product_image now handles image_url and description
                        if update_product_barcode(session, product_code, data):
                            updated_count += 1
                        else:
                            not_found_count += 1
                            log_step(task_id, f"⚠️ Product code '{product_code}' not found in local database")
                    elif response.status_code == 404:
                        not_found_count += 1
                        log_step(task_id, f"⚠️ Product code '{product_code}' not found in Go-UPC API")
                
                    else:
                        error_count += 1
                        log_step(task_id, f"❌ API Error for '{product_code}': Status {response.status_code}")        
            
                        
                except Exception as e:
                    error_count += 1
                    log_step(task_id, f"❌ Error updating product '{product_code}': {str(e)}")
            
            # Final summary
            log_step(task_id, f"")
            log_step(task_id, f"📊 Summary:")
            log_step(task_id, f"   ✅ Successfully updated: {updated_count}")
            log_step(task_id, f"   ⚠️ Products not found: {not_found_count}")
            log_step(task_id, f"   ❌ Errors: {error_count}")
            
        finally:
            session.close()
        
        end_time = datetime.datetime.now()
        log_step(task_id, f"📅 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_duration = end_time - start_time
        log_step(task_id, f"⏱️ Total Duration: {total_duration}")
        
        tasks[task_id]["status"] = "completed"
        log_step(task_id, "✅ Image update completed successfully.")
        
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        log_step(task_id, f"❌ Error: {str(e)}")
    finally:
        # Clean up the uploaded file
        if os.path.exists(file_path):
            os.remove(file_path)            

@app.post("/import_external_images")
async def import_external_images(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting image import..."],
        "report_url": None
    }

    background_tasks.add_task(process_external_images, task_id, file_location)
    return {"task_id": task_id}

@app.get("/download/{task_id}")
def download_report(task_id: str):
    report_path = f"reports/{task_id}_report.xlsx"
    print(f"Report path: {report_path}")
    if os.path.exists(report_path):
        return FileResponse(report_path, filename="report.xlsx")
    return JSONResponse(content={"error": "Report not found"}, status_code=404)

@app.get("/", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})

@app.get("/jarir", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload_jarir.html", {"request": request})

@app.get("/jarir/import_metadata", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload_jarir_metadata.html", {"request": request})

@app.get("/rawabi/products", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload_rawabi_products.html", {"request": request})

@app.get("/rawabi/inventory", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload_rawabi_inventory.html", {"request": request})

@app.get("/import_external_images", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("import_external_images.html", {"request": request})

@app.get("/upload_images", response_class=HTMLResponse)
async def upload_images_form(request: Request):
    return templates.TemplateResponse("upload_images.html", {"request": request})

@app.post("/upload_images")
async def upload_images(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    file_location = f"temp/{task_id}_{file.filename}"
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tasks[task_id] = {
        "status": "processing",
        "logs": ["File received, starting image update..."],
        "report_url": None
    }

    background_tasks.add_task(process_images_file, task_id, file_location)
    return {"task_id": task_id}


@app.post("/upload_old", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logs = []
    try:
        df = pd.read_excel(file_path, header=None)
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

        for i, batch_df in enumerate(batches):
            session = SessionLocal()
            try:
                logs.append(f"Processing batch {i + 1}")
                product_codes = batch_df["item_code"].unique().tolist()
                existing_codes = get_existing_product_codes(session, product_codes)
                missing_products = batch_df[~batch_df["item_code"].isin(existing_codes)]
                products_to_insert = missing_products.apply(lambda row: {
                    "name": row["item_name"],
                    "item_code": row["item_code"],
                    "category_id": row.get("item_code", 3),
                    "cost_price": row["item_cost_price"],
                    "sale_price": row["item_sale_price"],
                }, axis=1).tolist()

                insert_missing_products(session, products_to_insert)
                create_purchase(session, batch_df)

                logs.append(f"Batch {i + 1} inserted successfully")
            except Exception as e:
                logs.append(f"Error in batch {i + 1}: {str(e)}")
                session.rollback()
            finally:
                session.close()

        logs.append("✅ All batches processed successfully.")
    except Exception as e:
        logs.append(f"❌ Failed to process file: {str(e)}")

    return templates.TemplateResponse("upload.html", {"request": request, "logs": logs})

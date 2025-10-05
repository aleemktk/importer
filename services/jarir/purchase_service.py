import pandas as pd
from sqlalchemy.orm import Session
from model import Product, Purchase, PurchaseItem, Inventory, Transfer
from datetime import datetime
import time
import random


def create_purchase(db: Session, batch_df) -> Purchase:
    
    grand_total_purchase = 0.0
    grand_total_net_purchase = 0.0
    grand_total_sale = 0.0
    grand_total = 0.0
    total_item = 0.0
    total_vat = 0.0

    total_tansfer_sale_vat = 0.0
    grand_transfer_total = 0.0

    supplier_id = 686
    supplier_name = 'Internal supplier'

    purchase_items = []
   

    from_warehouse_id = 32  # Example warehouse ID
    from_warehouse_name = "Abaad Asaha Warehouse"  # Example warehouse name
    from_warehouse_code = "2000"

    to_warehouse_id = 38
    to_warehouse_name = "صيدلية ابعاد الصحة الأولى "  # Example pharmacy name
    to_warehouse_code = "2005"
    
    # Insert purchase items
    for _, item_data in batch_df.iterrows(): 

        grand_total_purchase+= float(item_data['item_total_cost_price'])
        grand_total_net_purchase += float(item_data['item_total_cost_price'])
        grand_total_sale += float(item_data['item_total_sale_price']) 
        grand_total += float(item_data['item_total_sale_price'])
        total_item += float(item_data['item_quantity'])
        total_vat += float(item_data['item_total_vat'])   
        supplier_id = item_data['supplier_id']
        supplier_name = item_data['supplier_name']
        # for transfer calculation
        total_tansfer_sale_vat += float(item_data['total_sale_vat'])
        grand_transfer_total += float(item_data['total_sale'])

        # df["total_sale_vat"] = df["item_total_sale_price"] * df["vat_value"]
        # df["total_sale"] = df["item_total_sale_price"] + df["total_sale_vat"]

        expiry_date = None
        if pd.notnull(item_data['item_expiry_date']):
            try:
                expiry_date = datetime.strptime(str(item_data['item_expiry_date']), "%Y-%m-%d").date()
            except ValueError:
                try:
                    expiry_date = pd.to_datetime(item_data['item_expiry_date']).date()
                except Exception:
                    print(f"Invalid expiry date format for item_code {item_data['item_code']}: {item_data['item_expiry_date']}")
                    continue
        item_before_vat = item_data['item_total_cost_price']
        
        # timestamp = int(time.time() * 10000)  # equivalent to microtime(true) * 10000
        # random_number = random.randint(100, 999)  # equivalent to mt_rand(100, 999)
        # unique_code = str(timestamp) + str(random_number)
        # avz_item_code = unique_code[-6:]  # last 6 digits
        # avz_item_code = 
        purchase_items.append({
        "product_code": item_data["item_code"],
        "product_name": item_data["item_name"],
        "net_unit_cost": item_data["item_cost_price"],
        "quantity": item_data["item_quantity"],
        "item_tax": item_data["item_total_vat"],
        "discount": "",
        "item_discount": "",
        "expiry": expiry_date,
        "subtotal": item_data["item_total_cost_price"],
        "unit_cost": item_data["item_cost_price"],
        "real_unit_cost": item_data["item_cost_price"],
        "sale_price": item_data["item_sale_price"],
        "date": datetime.now().date(),
        "status": "received",
        "unit_quantity": item_data["item_quantity"],
        "quantity_balance": item_data["item_quantity"],
        "option_id": None,  # Adjust if needed
        "quantity_received": item_data["item_quantity"],
        "batchno": item_data["item_batch_number"],
        "serial_number": "",
        "bonus": 0,
        "discount1": "",
        "discount2": "",
        "totalbeforevat": item_before_vat,
        "main_net": item_data["item_total_after_vat"],
        "warehouse_shelf": "",
        "avz_item_code": item_data['item_ascon_code'],
        "second_discount_value": "",
        "transfer_subtotal": item_data["item_total_sale_price"],
        "transfer_total_tax": item_data["item_total_vat"],
        "transfer_main_net": item_data["total_sale"],
        "total_sale_vat": item_data["total_sale_vat"],
        "total_sale": item_data["total_sale"],
        "item_total_sale_price": item_data["item_total_sale_price"],
        })

    new_purchase = Purchase(
        reference_no='123456',
        date=datetime.now(),
        supplier_id=supplier_id,
        supplier=supplier_name,
        warehouse_id=from_warehouse_id,
        note="import from excel",
        total=grand_total_purchase,
        #old_total_net_purchase=purchase_data.old_total_net_purchase,
        total_net_purchase=grand_total_net_purchase,
        total_sale=grand_total_sale,
        product_discount='',
        order_discount_id='',
        order_discount='',
        total_discount='',
        product_tax='',
        order_tax_id='',
        order_tax='',
        total_tax=total_vat,
        shipping='',
        grand_total=grand_total,
        paid='',
        status='received',
        created_by=9,
        invoice_number='PR-123456',
        sequence_code='INV-123456',
    )
    db.add(new_purchase)
    db.flush()  # so we get new_purchase.id
   


    new_transfer = Transfer(
        transfer_no=new_purchase.reference_no,
        date=datetime.now(),
        from_warehouse_id=from_warehouse_id,
        from_warehouse_name=from_warehouse_name,
        from_warehouse_code=from_warehouse_code,
        to_warehouse_id=to_warehouse_id,
        to_warehouse_name=to_warehouse_name,   
        to_warehouse_code=to_warehouse_code,
        note="import from excel",
        attachment='',
        total=new_purchase.total_sale, 
        total_cost=new_purchase.total,
        total_tax=total_tansfer_sale_vat,
        grand_total=grand_transfer_total,
        type='purchase',
        status='completed',
        sequence_code='TR-123456',
        created_by=9,
        invoice_number='TR-INV-123456'
    )
    db.add(new_transfer)
    db.flush()  # so we get new_transfer.id
    # After creating a transfer

    
    for item_data in purchase_items:

        # Fetch product_id from database
        product = db.query(Product).filter_by(code=item_data['product_code']).first()
        product_id = product.id if product else None
       
        item = PurchaseItem(
            purchase_id=new_purchase.id,
            product_id=product_id,
            product_code=item_data['product_code'],
            product_name=item_data['product_name'],
            net_unit_cost=item_data['net_unit_cost'],
            quantity=item_data['quantity'],
            warehouse_id=new_purchase.warehouse_id,
            item_tax=item_data['item_tax'],
            discount='',
            item_discount='',
            expiry=item_data['expiry'],
            subtotal=item_data['subtotal'], 
            unit_cost=item_data['unit_cost'],
            real_unit_cost=item_data['real_unit_cost'],
            sale_price=item_data['sale_price'],
            date=datetime.now().date(),
            status='received',
            unit_quantity=item_data['unit_quantity'],
            quantity_balance=item_data['quantity_balance'],
            option_id=None,  # Adjust if needed
            quantity_received=item_data['quantity_received'],
            batchno=item_data['batchno'],
            serial_number='',
            bonus=0,
            discount1='',
            discount2='',
            totalbeforevat=item_data['totalbeforevat'],
            main_net=item_data['main_net'],
            warehouse_shelf='',
            avz_item_code=item_data['avz_item_code'],
            second_discount_value=''
        )

    
   
        inventory_item = Inventory(
            product_id = product_id,
            batch_number = item_data['batchno'],
            movement_date =new_purchase.date,
            type = 'purchase',
            quantity = item_data['quantity'],
            location_id = new_purchase.warehouse_id,
            net_unit_cost = item_data['unit_cost'],
            expiry_date = item_data['expiry'],
            net_unit_sale = item_data['sale_price'],
            reference_id = new_purchase.id,
            real_unit_cost = item_data['real_unit_cost'],
            real_unit_sale = item_data['sale_price'],
            avz_item_code = item_data['avz_item_code'],
            bonus = 0,
            customer_id = ""
        )

        # make transfer item and inventory item to the session
        transfer_item = PurchaseItem(
            transfer_id=new_transfer.id,
            product_id=product_id,
            product_code=item_data['product_code'],
            product_name=item_data['product_name'],
            net_unit_cost=item_data['net_unit_cost'],
            quantity=item_data['quantity'],
            warehouse_id=new_transfer.to_warehouse_id,
            item_tax=item_data['total_sale_vat'],
            discount='',
            item_discount='',
            expiry=item_data['expiry'],
            subtotal=item_data['total_sale'], 
            unit_cost=item_data['unit_cost'],
            real_unit_cost=item_data['real_unit_cost'],
            sale_price=item_data['sale_price'],
            date=datetime.now().date(),
            status='completed',
            unit_quantity=item_data['unit_quantity'],
            quantity_balance=item_data['quantity_balance'],
            option_id=None,  # Adjust if needed
            quantity_received=item_data['quantity_received'],
            batchno=item_data['batchno'],
            serial_number='',
            bonus=0,
            discount1='',
            discount2='',
            totalbeforevat=item_data['item_total_sale_price'],
            main_net=item_data['transfer_main_net'],
            warehouse_shelf='',
            avz_item_code=item_data['avz_item_code'],
            second_discount_value=''
        )

        transfer_out_inventory_item = Inventory(
            product_id = product_id,
            batch_number = item_data['batchno'],
            movement_date =new_transfer.date,
            type = 'transfer_out',
            quantity = -1* item_data['quantity'],
            location_id = new_transfer.from_warehouse_id,
            net_unit_cost = item_data['unit_cost'],
            expiry_date = item_data['expiry'],
            net_unit_sale = item_data['sale_price'],
            reference_id = new_transfer.id,
            real_unit_cost = item_data['real_unit_cost'],
            real_unit_sale = item_data['sale_price'],
            avz_item_code = item_data['avz_item_code'],
            bonus = 0,
            customer_id = ""
        )

        transfer_in_inventory_item = Inventory(
            product_id = product_id,
            batch_number = item_data['batchno'],
            movement_date =new_transfer.date,
            type = 'transfer_in',
            quantity = item_data['quantity'],
            location_id = new_transfer.to_warehouse_id,
            net_unit_cost = item_data['unit_cost'],
            expiry_date = item_data['expiry'],
            net_unit_sale = item_data['sale_price'],
            reference_id = new_transfer.id,
            real_unit_cost = item_data['real_unit_cost'],
            real_unit_sale = item_data['sale_price'],
            avz_item_code = item_data['avz_item_code'],
            bonus = 0,
            customer_id = ""
        )

        # Add purchase item, purchase item, and inventory items to the session
        db.add(item)
        db.add(inventory_item)

        # Add purchase item, transfer item, and inventory items to the session
        # db.add(transfer_item)
        # db.add(transfer_in_inventory_item)
        # db.add(transfer_out_inventory_item)
        


    db.commit()
    return {
    "purchase_id": new_purchase.id,
    #"transfer_id": new_transfer.id
   }

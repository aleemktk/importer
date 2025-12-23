import pandas as pd
from sqlalchemy.orm import Session
from model import Product, Purchase, PurchaseItem, Inventory, Transfer, Supplier
from schemas import PurchaseCreateSchema, PurchaseItemCreateSchema
from datetime import datetime
from sqlalchemy import text


import random
generated_codes = set()

def generate_unique_item_code():
    while True:
        code = str(random.randint(100000, 999999))  # 6-digit code
        if code not in generated_codes:
            generated_codes.add(code)
            return code

def create_rawabi_purchase(db: Session, batch_df) -> Purchase:
    
    grand_total_purchase = 0.0
    grand_total_net_purchase = 0.0
    grand_total_sale = 0.0
    grand_total = 0.0
    total_item = 0.0
    total_vat = 0.0

    total_tansfer_sale_vat = 0.0
    grand_transfer_total = 0.0

    total_discount = 0.0

    purchase_items = []
   

    from_warehouse_id = 32  # Example warehouse ID
    from_warehouse_name = "Pharma Warehouse"  # Example warehouse name
    from_warehouse_code = "2000"
    
    # Insert purchase items
    for _, item_data in batch_df.iterrows(): 

        grand_total_purchase+= float(item_data['item_total_cost_price'])
        grand_total_net_purchase += float(item_data['item_total_cost_price'])
        grand_total_sale += float(item_data['item_total_sale_price']) 
        grand_total += float(item_data['item_total_after_vat']) 
        total_item += float(item_data['item_quantity'])
        total_vat += float(item_data['item_total_vat'])   
        # for transfer calculation
        total_tansfer_sale_vat += float(item_data['total_sale_vat'])

        item_data['total_sale'] = 0 if pd.isna(item_data['total_sale']) else item_data['total_sale']

        grand_transfer_total += float(item_data['total_sale'])


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
        
        discount_rate = float(item_data["item_discount"]) or 0

        # price after discount
        price_after_discount = float(item_data["item_cost_price"]) or 0

        if discount_rate > 0:
            price_before_discount = price_after_discount / (1 - discount_rate)
            discount_value = float(price_before_discount - price_after_discount)
            discount_percentage = discount_rate * 100
        else:
            price_before_discount = price_after_discount
            discount_value = 0.0
            discount_percentage = ''

        total_discount += float(discount_value)


        purchase_items.append({
        "product_code": item_data["item_code"],
        "product_name": item_data["item_name"],
        "net_unit_cost": item_data["item_cost_price"],
        "quantity": item_data["item_quantity"],
        "item_tax": (item_data["item_cost_price"] * item_data["item_quantity"]) * item_data["vat_value"],
        "discount": discount_percentage,
        "item_discount": discount_value,
        "expiry": expiry_date,
        "subtotal": item_data["item_total_cost_price"],
        "unit_cost": item_data["item_cost_price"],
        "real_unit_cost": item_data["item_purchase_price"],
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
        "discount1": discount_value,
        "discount2": "",
        "totalbeforevat": item_before_vat,
        "main_net": item_data["item_total_after_vat"],
        "warehouse_shelf": '', #item_data['item_location_number'],
        "avz_item_code": generate_unique_item_code(),
        "second_discount_value": "",
        "transfer_subtotal": item_data["item_total_sale_price"],
        "transfer_total_tax": item_data["item_total_vat"],
        "transfer_main_net": item_data["total_sale"],
        "total_sale_vat": item_data["total_sale_vat"],
        "total_sale": item_data["total_sale"],
        "item_total_sale_price": item_data["item_total_sale_price"],
        })

    #   get supplier id from supplier tablse based on supplier_id
    # item_data['supplier_id'] = db.query(Supplier).filter( Supplier.group_id == 4 and Supplier.external_id== item_data['supplier_id']).first().id

    new_purchase = Purchase(
        reference_no='123456',
        date=datetime.now(), 
        supplier_id=item_data['supplier_id'],
        supplier=item_data['supplier_name'],
        warehouse_id=from_warehouse_id,
        note="import from excel",
        total=grand_total_purchase,
        old_total_net_purchase=0.0,
        total_net_purchase=grand_total_net_purchase,
        total_sale=grand_total_sale,
        product_discount=0.0,
        order_discount_id='',
        order_discount=0.0,
        total_discount=total_discount,
        product_tax=0.0,
        order_tax_id=0,
        order_tax=0.0,
        total_tax=total_vat,
        shipping=0.0,
        grand_total=grand_total,
        paid=0.0,
        status='received',
        created_by=9,
        invoice_number='PR-123456',
        sequence_code='INV-123456',
    )
    db.add(new_purchase)
    db.flush()  # so we get new_purchase.id
    # Khaled's Script Addition Starts Here
    # Also insert into sma_purchase_orders
    sma_purchase_order_sql = text("""
        INSERT INTO sma_purchase_orders (
            reference_no, date, supplier_id, supplier, warehouse_id, note,
            total, old_total_net_purchase, total_net_purchase, total_sale,
            product_discount, order_discount_id, order_discount, total_discount,
            product_tax, order_tax_id, order_tax, total_tax, shipping,
            grand_total, paid, status, created_by, invoice_number, sequence_code,
            purchase_id
        ) VALUES (
            :reference_no, :date, :supplier_id, :supplier, :warehouse_id, :note,
            :total, :old_total_net_purchase, :total_net_purchase, :total_sale,
            :product_discount, :order_discount_id, :order_discount, :total_discount,
            :product_tax, :order_tax_id, :order_tax, :total_tax, :shipping,
            :grand_total, :paid, :status, :created_by, :invoice_number, :sequence_code,
            :purchase_id
        )
    """)
    
    db.execute(sma_purchase_order_sql, {
        'reference_no': new_purchase.reference_no,
        'date': new_purchase.date,
        'supplier_id': new_purchase.supplier_id,
        'supplier': new_purchase.supplier,
        'warehouse_id': new_purchase.warehouse_id,
        'note': new_purchase.note,
        'total': new_purchase.total,
        'old_total_net_purchase': new_purchase.old_total_net_purchase,
        'total_net_purchase': new_purchase.total_net_purchase,
        'total_sale': new_purchase.total_sale,
        'product_discount': new_purchase.product_discount,
        'order_discount_id': new_purchase.order_discount_id,
        'order_discount': new_purchase.order_discount,
        'total_discount': new_purchase.total_discount,
        'product_tax': new_purchase.product_tax,
        'order_tax_id': new_purchase.order_tax_id,
        'order_tax': new_purchase.order_tax,
        'total_tax': new_purchase.total_tax,
        'shipping': new_purchase.shipping,
        'grand_total': new_purchase.grand_total,
        'paid': new_purchase.paid,
        'status': "pending",
        'created_by': new_purchase.created_by,
        'invoice_number': new_purchase.invoice_number,
        'sequence_code': new_purchase.sequence_code,
        'purchase_id': new_purchase.id
    })
    db.flush()
    
    # Get the inserted sma_purchase_order id
    sma_po_id = db.execute(text("SELECT LAST_INSERT_ID()")).scalar()


    # Khaled's Script Addition Ends Here
    for item_data in purchase_items:

        # Fetch product_id from database
        product = db.query(Product).filter_by(code=item_data['product_code']).first()
        print(f"Product fetched for code {item_data['product_code']}: {product}")
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
            discount=item_data['discount'],
            item_discount=item_data['item_discount'],
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
            discount1=item_data['discount1'],
            discount2=0.0,
            totalbeforevat=item_data['totalbeforevat'],
            main_net=item_data['main_net'],
            warehouse_shelf='',
            avz_item_code=item_data['avz_item_code'],
            second_discount_value=0.0
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
            customer_id = 0
        )


        # Add purchase item, purchase item, and inventory items to the session
        db.add(item)
        # db.add(inventory_item)  # COMMENTED: Inventory update disabled

        # Also insert into sma_purchase_order_items
        sma_po_item_sql = text("""
            INSERT INTO sma_purchase_order_items (
                purchase_id, product_id, product_code, product_name, option_id,
                net_unit_cost, quantity, actual_quantity, warehouse_id, item_tax, discount,
                item_discount, expiry, subtotal, quantity_balance, date, status,
                unit_cost, real_unit_cost, sale_price, quantity_received,
                unit_quantity, batchno, serial_number, bonus, discount1, discount2,
                totalbeforevat, main_net, warehouse_shelf, avz_item_code,
                second_discount_value
            ) VALUES (
                :purchase_id, :product_id, :product_code, :product_name, :option_id,
                :net_unit_cost, :quantity, :actual_quantity, :warehouse_id, :item_tax, :discount,
                :item_discount, :expiry, :subtotal, :quantity_balance, :date, :status,
                :unit_cost, :real_unit_cost, :sale_price, :quantity_received,
                :unit_quantity, :batchno, :serial_number, :bonus, :discount1, :discount2,
                :totalbeforevat, :main_net, :warehouse_shelf, :avz_item_code,
                :second_discount_value
            )
        """)
        
        db.execute(sma_po_item_sql, {
            'purchase_id': sma_po_id,
            'product_id': product_id,
            'product_code': item_data['product_code'],
            'product_name': item_data['product_name'],
            'option_id': None,
            'net_unit_cost': item_data['net_unit_cost'],
            'quantity': item_data['quantity'],
            'actual_quantity': item_data['quantity'],
            'warehouse_id': new_purchase.warehouse_id,
            'item_tax': item_data['item_tax'],
            'discount': item_data['discount'],
            'item_discount': item_data['item_discount'],
            'expiry': item_data['expiry'],
            'subtotal': item_data['subtotal'],
            'quantity_balance': item_data['quantity_balance'],
            'date': datetime.now().date(),
            'status': 'received',
            'unit_cost': item_data['unit_cost'],
            'real_unit_cost': item_data['real_unit_cost'],
            'sale_price': item_data['sale_price'],
            'quantity_received': item_data['quantity_received'],
            'unit_quantity': item_data['unit_quantity'],
            'batchno': item_data['batchno'],
            'serial_number': '',
            'bonus': 0,
            'discount1': item_data['discount1'],
            'discount2': 0.0,
            'totalbeforevat': item_data['totalbeforevat'],
            'main_net': item_data['main_net'],
            'warehouse_shelf': '',
            'avz_item_code': item_data['avz_item_code'],
            'second_discount_value': 0.0
        })

        # Update product's cost and price in stock
        product = db.query(Product).filter_by(code=item_data['product_code']).first()
        if product:
            product.cost = item_data['unit_cost']
            product.price = item_data['sale_price']
            db.add(product)

     

    db.commit()
    return {
    "purchase_id": new_purchase.id,
    "transfer_id": 0
   }

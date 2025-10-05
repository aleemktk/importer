from sqlalchemy import Column, Integer, String, Float, ForeignKey, Date, DateTime, func, Numeric,TIMESTAMP, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Product(Base):
    __tablename__ = 'sma_products'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    item_code = Column(String(100), unique=True, nullable=False)
    code = Column(String(100), unique=True, nullable=False)
    cost = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    category_id = Column(Integer)
    tax_rate = (Column(Integer))


# models/purchase.py

# from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Text
# from sqlalchemy.sql import func
# from database import Base

class Purchase(Base):
    __tablename__ = 'sma_purchases'

    id = Column(Integer, primary_key=True, autoincrement=True)
    reference_no = Column(String(55))
    date = Column(DateTime, server_default=func.current_timestamp())
    supplier_id = Column(Integer)
    supplier = Column(String(55))
    warehouse_id = Column(Integer)
    note = Column(String(1000))
    total = Column(Numeric(25, 5))
    total_net_purchase = Column(Numeric(25, 5))
    total_sale = Column(Numeric(25, 5))
    product_discount = Column(Numeric(25, 2))
    order_discount_id = Column(String(20))
    order_discount = Column(Numeric(25, 5))
    total_discount = Column(Numeric(25, 5))
    product_tax = Column(Numeric(25, 5))
    order_tax_id = Column(Integer)
    order_tax = Column(Numeric(25, 5))
    total_tax = Column(Numeric(25, 5))
    shipping = Column(Numeric(25, 2), default=0.00)
    grand_total = Column(Numeric(25, 5))
    paid = Column(Numeric(25, 5))
    status = Column(String(55), default='')
    purchase_invoice = Column(Integer, default=0)  # tinyint(1)
    invoice_number = Column(String(255))
    sequence_code = Column(String(255))
    created_by = Column(Integer)
    is_transfer = Column(Integer, nullable= True, default=0)  # tinyint(1)
    transfer_id = Column(Integer, nullable=True, default=0)
    location_to = Column(Integer, nullable=True, default=0)
    transfer_by = Column(Integer, nullable=True, default=0)
 
	
class Transfer(Base):
    __tablename__ = 'sma_transfers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    transfer_no = Column(String(55))
    date = Column(DateTime, server_default=func.current_timestamp())
    from_warehouse_id = Column(Integer)
    from_warehouse_code = Column(String(55))
    from_warehouse_name	= Column(String(255))	
    to_warehouse_code = Column(String(255))
    to_warehouse_id = Column(Integer)
    to_warehouse_code = Column(String(255))
    to_warehouse_name = Column(String(255))
    note = Column(String(1000), nullable=True)
    attachment = Column(String(55), nullable=True)
    total = Column(Numeric(25, 5))
    total_cost = Column(Numeric(25, 5))
    total_tax = Column(Numeric(25, 5))
    grand_total = Column(Numeric(25, 2))
    type = Column(String(10), nullable=True)
    sequence_code = Column(String(255), nullable=True)
    invoice_number = Column(String(255), nullable=True)
    status = Column(String(55), default='')
    created_by = Column(Integer)
 



class PurchaseItem(Base):
    __tablename__ = 'sma_purchase_items'

    id = Column(Integer, primary_key=True, index=True)
    purchase_id = Column(Integer, nullable=True)
    transfer_id = Column(Integer, nullable=True)
    product_id = Column(Integer, nullable=True)
    product_code = Column(String(50), nullable=True)
    product_name = Column(String(255), nullable=True)
    option_id = Column(Integer, nullable=True)
    net_unit_cost = Column(Numeric(25, 5), nullable=True)
    quantity = Column(Numeric(25, 5), nullable=False)
    warehouse_id = Column(Integer, nullable=False)
    item_tax = Column(Numeric(25, 5), nullable=True)
    tax_rate_id = Column(Integer, nullable=True)
    tax = Column(String(20), nullable=True)
    discount = Column(String(20), nullable=True)
    item_discount = Column(Numeric(25, 5), nullable=True)
    expiry = Column(Date, nullable=True)
    subtotal = Column(Numeric(25, 5), nullable=True)
    quantity_balance = Column(Numeric(25, 5), nullable=True)
    date = Column(Date, nullable=True)
    status = Column(String(50), nullable=True)
    unit_cost = Column(Numeric(25, 5), nullable=True)
    real_unit_cost = Column(Numeric(25, 5), nullable=True)
    sale_price = Column(Numeric(25, 5), nullable=True)
    quantity_received = Column(Numeric(25, 5), nullable=True)
    supplier_part_no = Column(String(50), nullable=True)
    purchase_item_id = Column(Integer, nullable=True)
    product_unit_id = Column(Integer, nullable=True)
    product_unit_code = Column(String(10), nullable=True)
    unit_quantity = Column(Numeric(25, 5), nullable=True)
    gst = Column(String(20), nullable=True)
    cgst = Column(Numeric(25, 4), nullable=True)
    sgst = Column(Numeric(25, 4), nullable=True)
    igst = Column(Numeric(25, 4), nullable=True)
    base_unit_cost = Column(Numeric(25, 4), nullable=True)
    subtotal2 = Column(Numeric(25, 4), nullable=True)
    batchno = Column(String(50), nullable=True)
    serial_number = Column(String(200), nullable=True)
    bonus = Column(Numeric(25, 2), nullable=True)
    discount1 = Column(Numeric(25, 5), nullable=True)
    discount2 = Column(Numeric(25, 2), nullable=True)
    totalbeforevat = Column(Numeric(25, 5), nullable=True)
    main_net = Column(Numeric(25, 5), nullable=True)
    warehouse_shelf = Column(String(50), nullable=True)
    avz_item_code = Column(String(50), nullable=True)
    second_discount_value = Column(Numeric(25, 5), nullable=True)
    returned_quantity = Column(Numeric(25, 5), nullable=True)



class Inventory(Base):
    __tablename__ = 'sma_inventory_movements'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, index=True)
    batch_number = Column(String(255), index=True)
    movement_date = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(), index=True)
    type = Column(Enum('purchase', 'sale', 'transfer_in', 'transfer_out', 'adjustment'), nullable=False, index=True)
    quantity = Column(Numeric(25, 5))
    location_id = Column(Integer, index=True)
    net_unit_cost = Column(Numeric(25, 5))
    expiry_date = Column(Date)
    net_unit_sale = Column(Numeric(25, 5))
    reference_id = Column(Integer)
    real_unit_cost = Column(Numeric(25, 5))
    real_unit_sale = Column(Numeric(25, 5))
    avz_item_code = Column(String(50))
    bonus = Column(Integer)
    customer_id = Column(Integer)

class Supplier(Base):
    __tablename__ = "sma_companies"
    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer,  nullable=False)
    group_name = Column(String(255),  nullable=False)  
    name = Column(String(255),  nullable=False)  
    name_ar = Column(String(255),  nullable=False)  
    company = Column(String(255),  nullable=False)  

class Category(Base):
    __tablename__ = "sma_categories"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(255),  nullable=False)    
    name = Column(String(255),  nullable=False)    
    slug = Column(String(255),  nullable=False)  
    parent_id = Column(Integer,  nullable=False)
    description = Column(String(255),  nullable=False)  
    category_code = Column(Integer,  nullable=False)  

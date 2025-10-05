from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime

class PurchaseItemCreateSchema(BaseModel):
    product_id: int
    product_code: str
    product_name: str
    net_unit_cost: float
    quantity: float
    item_tax: Optional[float]
    discount: Optional[str]
    item_discount: Optional[float]
    expiry: Optional[date]
    subtotal: float
    unit_cost: Optional[float]
    real_unit_cost: Optional[float]
    sale_price: Optional[float]
    unit_quantity: float
    batchno: Optional[str]
    serial_number: Optional[str]
    bonus: Optional[float]
    discount1: Optional[float]
    discount2: Optional[float]
    totalbeforevat: Optional[float]
    main_net: Optional[float]
    warehouse_shelf: Optional[str]
    avz_item_code: Optional[str]
    second_discount_value: Optional[float]

class PurchaseCreateSchema(BaseModel):
    reference_no: str
    supplier_id: int
    supplier: str
    warehouse_id: int
    note: Optional[str]
    total: float
    old_total_net_purchase: float
    total_net_purchase: float
    total_sale: float
    product_discount: Optional[float]
    order_discount_id: Optional[str]
    order_discount: Optional[float]
    total_discount: Optional[float]
    product_tax: Optional[float]
    order_tax_id: Optional[int]
    order_tax: Optional[float]
    total_tax: Optional[float]
    shipping: Optional[float]
    grand_total: float
    paid: float
    status: Optional[str]
    payment_status: Optional[str]
    shelf_status: Optional[str]
    created_by: Optional[int]
    invoice_number: Optional[str]
    sequence_code: Optional[str]
    items: List[PurchaseItemCreateSchema]

from sqlalchemy.orm import Session
from model import Product

def get_existing_product_codes(session: Session, product_codes: list[str]) -> set[str]:
    """
    Get a set of product codes that already exist in the database.
    """
    existing = session.query(Product.item_code).filter(Product.item_code.in_(product_codes)).all()
    return {item_code for (item_code,) in existing}

def insert_missing_products(session: Session, products: list[dict]) -> None:
    """
    Insert missing products into the database.
    Each product dict should contain: name, code
    """
    if not products:
        return

    new_products = [Product(name=p['name'],
                             code=p['item_code'],
                               category_id=p['category_id'],
                               cost=p['cost_price'],
                               price=p['sale_price'],
                               item_code=p['item_code']
                               ) for p in products]
    session.bulk_save_objects(new_products)
    session.commit()

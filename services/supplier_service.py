from sqlalchemy.orm import Session
from model import Supplier

def get_existing_suppliers(session: Session, suppliers: list[str]) -> set[str]:
    """
    Get a set of product codes that already exist in the database.
    """
    existing = session.query(Supplier.name).filter(Supplier.name.in_(suppliers)).all()
    return {name for (name,) in existing}

def insert_missing_suppliers(session: Session, suppliers: list[dict]) -> None:
    """
    Insert missing products into the database.
    Each product dict should contain: name, code
    """
    if not suppliers:
        return
    new_suppliers = [Supplier(group_id=4,
                             group_name='supplier',
                               name=p['name'],
                               name_ar=p['name'],
                               company='Jarir'
                               ) for p in suppliers]
    session.bulk_save_objects(new_suppliers)
    session.commit()

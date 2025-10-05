from sqlalchemy.orm import Session
from model import Category

def get_existing_categories(session: Session, categories: list[str]) -> set[str]:
    """
    Get a set of product codes that already exist in the database.
    """
    existing = session.query(Category.name).filter(Category.name.in_(categories)).all()
    return {name for (name,) in existing}

def insert_missing_categories(session: Session, categories: list[dict]) -> None:
    """
    Insert missing products into the database.
    Each product dict should contain: name, code
    """
    if not categories:
        return
    new_categories = [Category(
                               code=p['name'],
                               name=p['name'],
                               slug=[p['name'].replace(" ", "-").lower()],
                               description=p['name'],
                               category_code=1,
                               parent_id = p['parent_id']
                               ) for p in categories]
    session.bulk_save_objects(new_categories)
    session.commit()


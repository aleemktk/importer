from sqlalchemy import text
from sqlalchemy.orm import Session

def update_product_image(session: Session, product_code: str, image_url: str) -> bool:
    """
    Update the image_url_new column for a product if it exists in sma_products table.
    
    Args:
        session: Database session
        product_code: Product code to search for
        image_url: Image URL to store
        
    Returns:
        True if product was found and updated, False otherwise
    """
    try:
        # Check if product exists and update image_url_new
        query = text("""
            UPDATE sma_products 
            SET image_url_new = :image_url 
            WHERE code = :product_code
        """)
        
        result = session.execute(query, {
            "image_url": image_url,
            "product_code": product_code
        })
        
        session.commit()
        
        # Return True if any rows were affected
        return result.rowcount > 0
        
    except Exception as e:
        session.rollback()
        raise e

def check_product_exists(session: Session, product_code: str) -> bool:
    """
    Check if a product code exists in sma_products table.
    
    Args:
        session: Database session
        product_code: Product code to check
        
    Returns:
        True if product exists, False otherwise
    """
    query = text("""
        SELECT COUNT(*) as count 
        FROM sma_products 
        WHERE code = :product_code
    """)
    
    result = session.execute(query, {"product_code": product_code})
    count = result.fetchone()[0]
    
    return count > 0

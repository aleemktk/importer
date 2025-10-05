import pandas as pd
from datetime import datetime
import os

def generate_import_report(imported_data: pd.DataFrame, report_dir: str = "reports") -> str:
    """
    Create an Excel report from imported data, grouped by product, with total cost and sale.
    Returns path to the generated file.
    """
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    report_df = imported_data.groupby(['product_code', 'product_name']).agg(
        total_quantity=('quantity', 'sum'),
        total_cost_price=('cost_price', 'sum'),
        total_sale_price=('sale_price', 'sum')
    ).reset_index()

    report_file = os.path.join(report_dir, f"import_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    report_df.to_excel(report_file, index=False)
    return report_file

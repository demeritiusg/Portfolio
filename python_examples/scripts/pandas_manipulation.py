import pandas as pd
import logging
import os

# Configure Logging
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] %(message)s"
)

def transform_data(
  sales_data_path: str,
  product_info_path: str,
  output_dir: str = "output"
):
  """
  Cleans, enriches, and transforms sales data by merging with product metadata
  and creating aggregated pivot tables for analysis.

  Parameters
  ----------
  sales_data_paht : str
    Path to raw sales CSV file.
  product_info_path : str
    Path to product metadata CSV file.
  output_dir : str, optional
    Directory to save transformed outputs. Default is 'output'

  Returns
  -------
  transformed_data : pd.DataFrame
    cleaned and enriched transactional dataset.
  pivot_table : pd.DataFrame
    Aggregated pivot table of sales by product and store.
  """

  # Load Data
  logging.info("Loading datasets...")
  try:
    sales_data = pd.read_csv(sales_data_path, parse_dates=['date'])
    product_info = pd.read_csv(product_info_path, index_col="product_id")
  except Exception as e:
    logging.error(f"Error loading input files: {e}")
    raise
  
  logging.info(f"Sales data: {sales_data.shape[0]} rows, {sales_data.shape[1]} cols")
  logging.info(f"Product data: {product_info.shape[0]} rows, {product_info.shape[1]} cols")
  
  # Data Quality Checks
  if sales_data['product_id'].isnull().any():
    logging.warning("Sales data contains missing product IDs.")
  if not sales_data['product_id'].isin(product_info_index).all():
    missing = sales_data.iloc[~sales_data['product_id'].isin(product_info.index), 'product_id']
    logging.warning(f"Sales data contains unknown product IDs: {missing}")
  
  # Merge Data
  logging.info("Merging sales data with product metadata...")
  transformed_data = sales_data.merge(
    product_info,
    left_on="product_id",
    right_index=True,
    how="left"
  )
  
  # Feature Engineering
  logging.info("Extracting temporal features...")
  transformed_data['year'] = tramsformed_data['date'].dt.year
  transformed_data['month'] = transformed_data['date'].dt.month
  
  # Aggregation
  logging.info("Creating pivot table (sales units by product and store)...")
  pivot_table = transformed_data.pivot_table(
    index="product_id",
    columns="store_id",
    values="sales_units",
    aggfunc="sum",
    full_value=0
  )
  
  # Ensure Output Directory
  os.makedirs(output_dir, exist_ok=True)
  
  # Save Outputs
  transformed_path = os.path.join(output_dir, "transformed_data.csv")
  pivot_path = os.path.join(output_dir, "pivot_table.csv")
  
  transformed_data.to_csv(transformed_path, index=False)
  pivot_table.to_csv(pivot_path)
  
  logging.info(f"Tansformed data save to {transformed_path}")
  logging.info(f"Pivot table save to {pivot_path}")
  
  return transformed_data, pivot_table

if __name__ == "__main__":
  transform_data()




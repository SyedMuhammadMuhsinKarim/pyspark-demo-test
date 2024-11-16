from src.utils.parquet_handler import DynamicParquetWriter
from datetime import datetime

app = DynamicParquetWriter()
num_rows = 100000 
num_profiles = 5000
start_date = datetime(2020, 1, 1)  
end_date = datetime(2023, 1, 1) 
path = "./data/bulk_data.parquet"

bulk_data = app.generate_bulk_data(num_rows, num_profiles, start_date, end_date)
app.save_to_parquet(bulk_data, path)
app.spark.stop()
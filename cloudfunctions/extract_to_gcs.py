#To extract data from nsepython library to Cloud storage
import base64
import functions_framework
import concurrent.futures
import time
import pandas as pd
from google.cloud import storage
from datetime import datetime,timedelta
from nsepython import nse_eq

start = time.time()

# Define stock symbols
stocks = [
    'WIPRO', 'INFY', 'TCS', 'TECHM', 'HDFCBANK', 'SUNPHARMA', 'BAJAJFINSV', 'HCLTECH', 'BAJFINANCE',
    'KOTAKBANK', 'MARUTI', 'INDUSINDBK', 'GRASIM', 'BRITANNIA', 'CIPLA', 'HEROMOTOCO', 'DRREDDY',
    'NESTLEIND', 'EICHERMOT', 'SHRIRAMFIN', 'RELIANCE', 'M&M', 'BHARTIARTL', 'ICICIBANK', 'ULTRACEMCO',
    'TITAN', 'ASIANPAINT', 'HINDUNILVR', 'HINDALCO', 'SBILIFE', 'ITC', 'APOLLOHOSP', 'ADANIENT',
    'BAJAJ-AUTO', 'ADANIPORTS', 'COALINDIA', 'TATACONSUM', 'LT', 'TATASTEEL', 'NTPC', 'BPCL', 'SBIN',
    'ONGC', 'JSWSTEEL', 'HDFCLIFE', 'AXISBANK', 'POWERGRID', 'TRENT', 'TATAMOTORS', 'BEL'
]

# Get the current date
current_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_stock_data(stock_symbol):
    """Fetch stock data from NSE API and extract required fields."""
    json_data = nse_eq(stock_symbol)
    extracted_data = {
        "symbol": json_data['info']['symbol'],
        "companyName": json_data['info']['companyName'],
        "industry": json_data['industryInfo']['industry'],
        "previousClose": json_data['priceInfo']['previousClose'],
        "open": json_data['priceInfo']['open'],
        "close": json_data['priceInfo']['close'],
        "buyQty": json_data['preOpenMarket']['totalBuyQuantity'],
        "sellQty": json_data['preOpenMarket']['totalSellQuantity'],
        "date": current_date  # Add current date column
    }
    return extracted_data

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

def uploadfile():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        data = list(executor.map(fetch_stock_data, stocks))

    # Create DataFrame
    df = pd.DataFrame(data)

    # Generate a timestamped file name for the Parquet file
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    parquet_file = f"nifty50data_{timestamp}.parquet"

    # Convert DataFrame to Parquet
    df.to_parquet(parquet_file, engine="fastparquet", index=False)
    bucket_name = "nsestock"  # Replace with your GCS bucket name
    destination_blob_name = f"raw/{parquet_file}"  # Upload to the "raw" folder
    upload_to_gcs(bucket_name, parquet_file, destination_blob_name)
    return ("success",200)
# Triggered from a message on a Cloud Pub/Sub topic.

@functions_framework.http
def hello_http(request):
    return uploadfile()

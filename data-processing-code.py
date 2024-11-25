# config.py
from datetime import datetime
import os

# S3 Configuration
S3_CONFIG = {
    'region': 'us-east-1',
    'buckets': {
        'source': 'lighthouse-stage',
        'paths': {
            'excel': 'lighthouse-new/excel-files/',
            'csv': 'lighthouse-new/csv-files/',
            'normalized': 'lighthouse-new/normalized-files/'
        }
    }
}

# Column mapping configuration
COLUMN_MAPPING = {
    'Date Pulled': 'date_pulled',
    'Date': 'date',
    'South Demand level': 'south_demand_level',
    'OPAL Price': 'opal_price',
    'OPAL Price Level': 'opal_price_level',
    'South price level': 'south_price_level',
    'Central Demand level': 'central_demand_level',
    'Hilton Price': 'hilton_price',
    'Hilton Price Level': 'hilton_price_level',
    'Central price level': 'central_price_level',
    'Central Flight level': 'central_flight_level',
    'South META Search Level': 'south_meta_search_level',
    'Central META Search Level': 'central_meta_search_level',
    'South GDS Search Level': 'south_gds_search_level',
    'Central GDS Search Level': 'central_gds_search_level',
    'South Unavailable Hotels': 'south_unavailable_hotels',
    'Central Unavailable Hotels': 'central_unavailable_hotels',
    'South Demand level2': 'south_demand_level2',
    'South price level3': 'south_price_level3',
    'Central Demand level4': 'central_demand_level4',
    'Central price level5': 'central_price_level5',
    'South META Search Level2': 'south_meta_search_level2',
    'Central META Search Level3': 'central_meta_search_level3',
    'South GDS Search Level2': 'south_gds_search_level2',
    'Central GDS Search Level3': 'central_gds_search_level3',
    'SOUTH Search Level': 'south_search_level',
    'CENTRAL Search Level': 'central_search_level',
    'Demand level': 'demand_level',
    'price level': 'price_level',
    'Central Flight level 6': 'central_flight_level6',
    'META Search Demand Level': 'meta_search_demand_level',
    'Unavailable Hotels': 'unavailable_hotels',
    'Overall Search Demand Level': 'overall_search_demand_level'
}

# s3_operations.py
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Optional

class S3Operations:
    def __init__(self, region: str):
        self.s3_client = boto3.client('s3', region_name=region)
        self.logger = logging.getLogger(__name__)

    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        try:
            self.s3_client.download_file(bucket, key, local_path)
            return True
        except ClientError as e:
            self.logger.error(f"Error downloading file {key}: {str(e)}")
            return False

    def upload_file(self, local_path: str, bucket: str, key: str) -> bool:
        try:
            self.s3_client.upload_file(local_path, bucket, key)
            return True
        except ClientError as e:
            self.logger.error(f"Error uploading file {key}: {str(e)}")
            return False

    def list_files(self, bucket: str, prefix: str) -> Optional[list]:
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            self.logger.error(f"Error listing files in {prefix}: {str(e)}")
            return None

# data_processor.py
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Optional, Tuple

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def excel_to_csv(self, input_path: str, output_path: str) -> bool:
        try:
            df = pd.read_excel(input_path)
            df.to_csv(output_path, index=False)
            return True
        except Exception as e:
            self.logger.error(f"Error converting Excel to CSV: {str(e)}")
            return False

    def normalize_percentage(self, value: str) -> Optional[float]:
        if pd.isna(value) or value == '':
            return None
        try:
            return float(str(value).strip('%')) / 100
        except:
            return None

    def normalize_price(self, value: str) -> Optional[float]:
        if pd.isna(value) or value == '' or str(value).lower() == 'sold out':
            return None
        try:
            return float(value)
        except:
            return None

    def normalize_csv(self, input_path: str, output_path: str, filename: str) -> bool:
        try:
            # Read CSV file
            df = pd.read_csv(input_path)

            # Rename columns according to mapping
            df.rename(columns=COLUMN_MAPPING, inplace=True)

            # Add metadata columns
            df['file_name'] = filename
            df['file_date'] = datetime.now().strftime('%Y-%m-%d')
            df['date_created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            df['date_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # Normalize percentage fields
            percentage_columns = ['south_unavailable_hotels', 'central_unavailable_hotels']
            for col in percentage_columns:
                df[col] = df[col].apply(self.normalize_percentage)

            # Normalize price fields
            price_columns = ['opal_price', 'hilton_price']
            for col in price_columns:
                df[col] = df[col].apply(self.normalize_price)

            # Convert dates
            date_columns = ['date_pulled', 'date']
            for col in date_columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')

            # Clean up whitespace
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()

            # Save normalized CSV
            df.to_csv(output_path, index=False)
            return True
        except Exception as e:
            self.logger.error(f"Error normalizing CSV: {str(e)}")
            return False

# main.py
import os
import logging
from datetime import datetime

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_processing.log'),
            logging.StreamHandler()
        ]
    )

def main():
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Initialize components
    s3_ops = S3Operations(S3_CONFIG['region'])
    processor = DataProcessor()
    
    # Create temporary directory for processing
    temp_dir = 'temp_processing'
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # List Excel files in source bucket
        excel_files = s3_ops.list_files(
            S3_CONFIG['buckets']['source'],
            S3_CONFIG['buckets']['paths']['excel']
        )
        
        if not excel_files:
            logger.info("No Excel files found for processing")
            return
        
        for excel_file in excel_files:
            try:
                filename = os.path.basename(excel_file)
                logger.info(f"Processing file: {filename}")
                
                # Download Excel file
                local_excel_path = os.path.join(temp_dir, filename)
                if not s3_ops.download_file(
                    S3_CONFIG['buckets']['source'],
                    excel_file,
                    local_excel_path
                ):
                    continue
                
                # Convert to CSV
                local_csv_path = os.path.join(temp_dir, f"{os.path.splitext(filename)[0]}.csv")
                if not processor.excel_to_csv(local_excel_path, local_csv_path):
                    continue
                
                # Upload intermediate CSV
                csv_key = os.path.join(
                    S3_CONFIG['buckets']['paths']['csv'],
                    os.path.basename(local_csv_path)
                )
                if not s3_ops.upload_file(
                    local_csv_path,
                    S3_CONFIG['buckets']['source'],
                    csv_key
                ):
                    continue
                
                # Normalize CSV
                local_normalized_path = os.path.join(
                    temp_dir,
                    f"normalized_{os.path.basename(local_csv_path)}"
                )
                if not processor.normalize_csv(
                    local_csv_path,
                    local_normalized_path,
                    filename
                ):
                    continue
                
                # Upload normalized CSV
                normalized_key = os.path.join(
                    S3_CONFIG['buckets']['paths']['normalized'],
                    os.path.basename(local_normalized_path)
                )
                s3_ops.upload_file(
                    local_normalized_path,
                    S3_CONFIG['buckets']['source'],
                    normalized_key
                )
                
                logger.info(f"Successfully processed {filename}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
            
            finally:
                # Cleanup temporary files
                for temp_file in [local_excel_path, local_csv_path, local_normalized_path]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
    
    finally:
        # Cleanup temporary directory
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)

if __name__ == "__main__":
    main()

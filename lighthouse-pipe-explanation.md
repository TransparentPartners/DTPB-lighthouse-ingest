# Lighthouse Data Pipeline Documentation

## Overview
This script creates a Snowflake pipe that automatically loads lighthouse data from S3 into a staging table. The pipe transforms raw CSV data into a structured format with additional metadata columns.

## Pipeline Details

### Source and Destination
- **Source**: S3 bucket stage `@STAGING_BUSINESS_ANALYTICS_SOURCES.LIGHTHOUSE.S3_LIGHTHOUSE`
- **Destination**: `STAGING_BUSINESS_ANALYTICS_SOURCES.LIGHTHOUSE.LIGHTHOUSE_STAGING`
- **Configuration**: `auto_ingest=false` (manual execution required)

### Data Transformation
The pipe performs the following transformations during data load:

1. **Basic Column Mapping**
   - Maps 33 columns from the source CSV ($1 through $33)
   - Includes various metrics like demand levels, price levels, and search metrics
   - Covers both South and Central region data

2. **Metadata Enrichment**
   Adds four additional columns:
   - `*file*name`: Captures the complete source file name
   - `*FILE*DATE`: Extracts and parses date from filename (format: MM-DD-YYYY)
   - `*Processed*Time`: Timestamp of when the record was processed
   - `*date*updated`: Timestamp for tracking record updates

### Column Categories

1. **Date Information**
   - Date_Pulled
   - Date

2. **Regional Price Metrics**
   - OPAL_Price
   - OPAL_Price_Level
   - Hilton_Price
   - Hilton_Price_Level
   - South_price_level
   - Central_price_level

3. **Demand Indicators**
   - South_Demand_level
   - Central_Demand_level
   - *Demand*level (overall)

4. **Search Metrics**
   - South_META_Search_Level
   - Central_META_Search_Level
   - South_GDS_Search_Level
   - Central_GDS_Search_Level
   - Overall_Search_Demand_Level

5. **Availability Metrics**
   - South_Unavailable_Hotels
   - Central_Unavailable_Hotels
   - Unavailable_Hotels

### Error Handling
- Uses `ON_ERROR = 'CONTINUE'`: The pipe will continue loading even if individual records fail
- Invalid records are skipped without stopping the entire load process

### File Format
- Expects CSV format input files
- Uses default CSV parsing settings

## Usage Notes

1. The pipe must be manually triggered since auto_ingest is disabled
2. Files should be named with the date format MM-DD-YYYY for proper date extraction
3. All source files should maintain the 33-column structure
4. The pipe adds tracking and audit columns automatically

## Best Practices

1. Monitor the error logs regularly since the pipe continues on error
2. Validate the date extraction from filenames
3. Consider implementing data quality checks on critical metrics
4. Review the metadata columns to track processing history


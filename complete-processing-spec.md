# Data Processing Pipeline Specification - S3 Excel to Snowflake

## 1. Overview
This specification details the end-to-end process of collecting Excel files from S3, converting them to CSV, normalizing the data, and preparing it for Snowflake database ingestion.

## 2. Source and Destination Details

### 2.1 S3 Locations
- **Source Excel Files**: `s3://lighthouse-stage/lighthouse-new/excel-files/`
- **Intermediate CSV Storage**: `s3://lighthouse-stage/lighthouse-new/csv-files/`
- **Final Normalized Files**: `s3://lighthouse-stage/lighthouse-new/normalized-files/`

### 2.2 Infrastructure
- AWS Region: `us-east-1`
- Storage Type: S3 Buckets
- Final Target: Snowflake Database

## 3. Process Flow

### 3.1 Stage 1: Excel to CSV Conversion
1. **File Collection**
   - Monitor source S3 bucket for new Excel files
   - Download Excel files for processing
   - Validate file integrity and format
   
2. **Conversion Process**
   - Convert Excel files to CSV format
   - Maintain original data structure
   - Preserve all data types
   - Handle multi-sheet workbooks
   
3. **Output Storage**
   - Store converted CSV files in intermediate location
   - Maintain original filename with `.csv` extension
   - Implement versioning for duplicate filenames

### 3.2 Stage 2: CSV Normalization
1. **Column Header Mapping**
   ```
   # Original Data Fields
   date_pulled           <- Date Pulled
   date                  <- Date
   south_demand_level    <- South Demand level
   opal_price           <- OPAL Price
   opal_price_level     <- OPAL Price Level
   south_price_level    <- South price level
   central_demand_level <- Central Demand level
   hilton_price         <- Hilton Price
   hilton_price_level   <- Hilton Price Level
   central_price_level  <- Central price level
   central_flight_level <- Central Flight level
   south_meta_search_level <- South META Search Level
   central_meta_search_level <- Central META Search Level
   south_gds_search_level <- South GDS Search Level
   central_gds_search_level <- Central GDS Search Level
   south_unavailable_hotels <- South Unavailable Hotels
   central_unavailable_hotels <- Central Unavailable Hotels
   south_demand_level2 <- South Demand level2
   south_price_level3 <- South price level3
   central_demand_level4 <- Central Demand level4
   central_price_level5 <- Central price level5
   south_meta_search_level2 <- South META Search Level2
   central_meta_search_level3 <- Central META Search Level3
   south_gds_search_level2 <- South GDS Search Level2
   central_gds_search_level3 <- Central GDS Search Level3
   south_search_level <- SOUTH Search Level
   central_search_level <- CENTRAL Search Level
   demand_level <- Demand level
   price_level <- price level
   central_flight_level6 <- Central Flight level 6
   meta_search_demand_level <- META Search Demand Level
   unavailable_hotels <- Unavailable Hotels
   overall_search_demand_level <- Overall Search Demand Level

   # Metadata Fields
   file_name              <- [Extracted from source file name]
   file_date              <- [Extracted from source file name]
   date_created           <- CURRENT_TIMESTAMP()
   date_modified          <- CURRENT_TIMESTAMP()
   ```

2. **Data Type Standardization**
   - Convert percentage values to decimal format (remove % symbol)
   - Standardize dates to YYYY-MM-DD format
   - Convert "Sold out" text to NULL values for price fields
   - Remove trailing/leading spaces
   - Convert empty strings to NULL
   - Format timestamps in Snowflake TIMESTAMP_NTZ(9)

3. **Metadata Field Specifications**

   a. **file_name**
      - Data Type: VARCHAR(255)
      - Source: Original filename
      - Preserve case and spacing
      - Include extension
   
   b. **file_date**
      - Data Type: DATE
      - Format: YYYY-MM-DD
      - Source: Filename or metadata
      - Validation: Must be valid date
   
   c. **date_created**
      - Data Type: TIMESTAMP_NTZ(9)
      - Default: CURRENT_TIMESTAMP()
      - Format: YYYY-MM-DD HH:MI:SS.FFFFFFFFF
   
   d. **date_modified**
      - Data Type: TIMESTAMP_NTZ(9)
      - Default: CURRENT_TIMESTAMP()
      - Update: On record modification
      - Format: YYYY-MM-DD HH:MI:SS.FFFFFFFFF

## 4. Data Validation and Quality Checks

### 4.1 Pre-Processing Validation
- Verify Excel file format
- Check for password protection
- Validate sheet structure
- Confirm required columns present

### 4.2 Processing Validation
- Verify column count matches
- Validate data type conversions
- Check date formats
- Verify numeric conversions
- Validate percentage transformations

### 4.3 Post-Processing Validation
- Compare record counts
- Verify column normalization
- Validate metadata fields
- Check for duplicate records
- Verify NULL handling

## 5. Error Handling

### 5.1 File Processing Errors
- Log failed conversions
- Implement retry mechanism
- Error notification system
- Move failed files to error directory
- Maximum retry attempts: 3

### 5.2 Data Validation Errors
- Log invalid records
- Create error reports
- Invalid data handling rules
- Partial success processing
- Error categorization

## 6. Performance Requirements

### 6.1 Processing Times
- Excel to CSV: < 5 minutes/file
- Normalization: < 3 minutes/file
- Total pipeline: < 10 minutes/file

### 6.2 Resource Management
- Memory: < 4GB per process
- CPU: < 70% utilization
- Storage: 20% minimum free space
- Concurrent processing: Up to 5 files

## 7. Security Requirements

### 7.1 Data Protection
- S3 bucket encryption
- Secure file transfer
- Access logging
- Audit trail maintenance

### 7.2 Access Control
- IAM role-based access
- Least privilege principle
- Access monitoring
- Regular permission review

## 8. Monitoring and Logging

### 8.1 Process Monitoring
- File processing status
- Processing time tracking
- Resource utilization
- Success/failure rates
- Record counts

### 8.2 Logging Requirements
- Process start/end times
- Error conditions
- File movements
- Data transformations
- Access attempts

## 9. Recovery Procedures

### 9.1 Failure Recovery
- Automated retry mechanism
- Manual intervention process
- Data recovery procedures
- Rollback capabilities

### 9.2 Backup Requirements
- Source file retention: 30 days
- Processed file backup
- Log file archival
- Recovery point objective: 24 hours

## 10. Implementation Notes

### 10.1 Development Standards
- Python 3.8 or higher
- AWS SDK (boto3)
- Pandas for data processing
- Common logging format
- Code documentation requirements

### 10.2 Deployment
- CI/CD pipeline integration
- Environment configurations
- Dependency management
- Version control requirements

## 11. Documentation Requirements

### 11.1 Technical Documentation
- System architecture
- Process flow diagrams
- Error code reference
- API specifications
- Configuration guide

### 11.2 Operational Documentation
- Run book
- Troubleshooting guide
- Recovery procedures
- Monitoring guide
- Maintenance procedures

## 12. Testing Requirements

### 12.1 Test Types
- Unit testing
- Integration testing
- Performance testing
- Security testing
- Recovery testing

### 12.2 Test Coverage
- Happy path scenarios
- Error conditions
- Edge cases
- Performance benchmarks
- Security compliance

## 13. Maintenance and Support

### 13.1 Routine Maintenance
- Log rotation
- Cleanup procedures
- Performance optimization
- Security updates

### 13.2 Support Requirements
- Response time SLAs
- Issue prioritization
- Escalation procedures
- Communication protocols

## 14. Compliance and Auditing

### 14.1 Compliance Requirements
- Data retention policies
- Security standards
- Audit requirements
- Documentation standards

### 14.2 Audit Trail
- Process logging
- Data lineage
- Change tracking
- Access logging

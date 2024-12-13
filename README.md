# Excel to CSV Data Processing Pipeline

## Overview
This project implements an automated data processing pipeline that handles the conversion of Excel files to CSV format, performs data normalization, and prepares the data for Snowflake database ingestion. The pipeline processes files from S3 storage, applies standardization rules, and handles comprehensive error checking and logging.

## Table of Contents
1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [Architecture](#architecture)
7. [Error Handling](#error-handling)
8. [Logging](#logging)
9. [Testing](#testing)
10. [Maintenance](#maintenance)
11. [Troubleshooting](#troubleshooting)

## Features
- Excel to CSV conversion with format preservation
- Data normalization and standardization
- Automated S3 file handling
- Comprehensive error handling and retry mechanisms
- Detailed logging and monitoring
- Metadata field management
- Snowflake-compatible data formatting

## Prerequisites
- Python 3.8 or higher
- AWS Account with S3 access
- Required Python packages:
  - pandas
  - boto3
  - openpyxl
  - numpy
  - logging

## Installation
1. Clone the repository:
```bash
git clone <repository-url>
cd excel-csv-processor
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Configure AWS credentials:
```bash
aws configure
```

## Configuration
1. Update `config.py` with your S3 bucket information:
```python
S3_CONFIG = {
    'region': 'us-east-1',
    'buckets': {
        'source': 'your-bucket-name',
        'paths': {
            'excel': 'path/to/excel/files/',
            'csv': 'path/to/csv/files/',
            'normalized': 'path/to/normalized/files/'
        }
    }
}
```

2. Adjust column mappings if needed in `config.py`

## Usage
1. Basic execution:
```bash
python main.py
```

2. With specific configuration file:
```bash
python main.py --config custom_config.py
```

3. Running with logging:
```bash
python main.py --log-level DEBUG
```

## Architecture
The pipeline consists of four main components:

1. **S3 Operations** (`s3_operations.py`)
   - Handles all S3 interactions
   - File upload/download management
   - S3 listing and verification

2. **Data Processor** (`data_processor.py`)
   - Excel to CSV conversion
   - Data normalization
   - Field standardization
   - Metadata management

3. **Configuration** (`config.py`)
   - S3 configuration
   - Column mappings
   - Processing rules

4. **Main Process** (`main.py`)
   - Pipeline orchestration
   - Error handling
   - Logging coordination

## Error Handling
The pipeline implements comprehensive error handling:

- File validation errors
- Conversion failures
- S3 operation errors
- Data validation issues
- Retry mechanism for recoverable errors

Error logs are stored in `data_processing.log`

## Logging
Logging is implemented at multiple levels:

1. **Process Logging**
   - Start/end of processing
   - Key operation completion
   - Error conditions

2. **Data Validation Logging**
   - Format validation
   - Data type issues
   - Missing data

3. **Performance Logging**
   - Processing times
   - Resource usage
   - Operation durations

## Testing
Run the test suite:
```bash
python -m pytest tests/
```

Key test areas:
- Unit tests for each component
- Integration tests for the pipeline
- S3 operation mocking
- Error condition testing

## Maintenance
Regular maintenance tasks:

1. **Log Management**
   - Rotate logs regularly
   - Archive old logs
   - Monitor log size

2. **Performance Optimization**
   - Monitor processing times
   - Optimize resource usage
   - Update configurations

3. **Error Monitoring**
   - Review error logs
   - Update error handling
   - Adjust retry mechanisms

## Troubleshooting

### Common Issues and Solutions

1. **S3 Access Issues**
```bash
Error: Access Denied
Solution: Verify AWS credentials and bucket permissions
```

2. **Excel Conversion Failures**
```bash
Error: Invalid Excel Format
Solution: Verify file format and structure
```

3. **Memory Issues**
```bash
Error: MemoryError
Solution: Adjust batch size in config.py
```

### Debug Mode
Enable debug logging:
```bash
python main.py --debug
```

### Support
For additional support:
1. Check the logs in `data_processing.log`
2. Review error messages in console output
3. Contact system administrator for AWS access issues

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Submit pull request

## Authors
- John Lillard/Transparent Partners
- Nitay Kenigsztein/Transparent Partners

## Acknowledgments
- AWS SDK Documentation
- Pandas Documentation
- Python Community Resources

## Project Status
Active development - 1.1

# drive2excel-pipeline

Python scripts to gather data from Google Drive PDF files, extract tables, and export them to Excel for easy tracking and analysis.


## Branches & Scripts

- **main** → `drive2excel_pipeline_gen.py`  
  Works with any PDF containing tables, no fixed schema required.

- **specific-cols** → `drive2excel_pipeline_col.py`  
  Works with PDFs with a known table structure and specific columns.


## Usage

1. Place `token.json` and `credentials.json` in the project folder (these are ignored from GitHub).  
2. Run the desired script:

```bash
# Generic version
python drive2excel_pipeline_gen.py

# Specific columns version
python drive2excel_pipeline_col.py


## Dependencies

Install required Python packages:

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib openpyxl pandas camelot-py[cv]
```

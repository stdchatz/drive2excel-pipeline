import os
import pandas as pd
import camelot
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# google drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


def authenticate_drive():
    creds = None

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)


def list_pdfs_in_folder(service, folder_id):
    """
    List all pdf files inside the google drive folder.
    """
    query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])


def download_pdf(service, file_id, file_name, download_path='downloads'):
    """
    Download a pdf file from google drive into a local folder.
    """
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    request = service.files().get_media(fileId=file_id)
    file_path = os.path.join(download_path, file_name)

    with open(file_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

    return file_path


def pdf_to_dataframe(pdf_file):
    """
    Extract tables from each pdf using camelot (stream) and clean them into a consistent schema.
    """
    try:
        tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream", strip_text="\n")

        if not tables:
            return pd.DataFrame()

        # merge all tables from this pdf
        df_list = [t.df for t in tables]
        df = pd.concat(df_list, ignore_index=True)

        # remove empty rows
        df = df.dropna(how="all")

        # force consistent schema
        df.columns = ["City", "Full Amount", "Max Amount (w/utilities)",
                      "Max Amount (only rent)", "No (manual)", "Fixed Amount",
                      "Not Yet", "Total", "Percentage"]

        # remove duplicate header rows
        df = df[df["City"] != "Full Amount"]

        # add file name to track source
        df["Source_File"] = os.path.basename(pdf_file)

        # reorder columns so Source_File is first
        cols = ["Source_File"] + [col for col in df.columns if col != "Source_File"]
        df = df[cols]

        return df

    except Exception as e:
        print(f"Error processing {pdf_file}: {e}")
        return pd.DataFrame()


def merge_pdfs_to_excel(pdf_paths, output_file='merged_output.xlsx'):
    """
    Process multiple pdfs, clean them, and merge into one single excel file.
    """
    all_dfs = []

    for pdf in pdf_paths:
        df = pdf_to_dataframe(pdf)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_excel(output_file, index=False)
        print(f"Saved merged Excel to {output_file}")
    else:
        print("No valid data found in PDFs.")


def main():
    """
    Pipeline:
    1. Authenticate with drive
    2. List pdfs in folder
    3. Download them
    4. Parse & clean
    5. Merge into excel
    """
    folder_id = "1-fZes6KYrBMkED3Ny0co_A0Qdr-D7jOr"

    service = authenticate_drive()
    pdf_files = list_pdfs_in_folder(service, folder_id)
    print(f"Found {len(pdf_files)} PDFs in folder.")

    downloaded_pdfs = []
    for file in pdf_files:
        path = download_pdf(service, file['id'], file['name'])
        downloaded_pdfs.append(path)

    merge_pdfs_to_excel(downloaded_pdfs, output_file='merged_output.xlsx')


if __name__ == '__main__':
    main()
import os
import csv
import io
from datetime import datetime
from pyairtable import Api
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- Config (set these as environment variables in Railway) ---
AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
AIRTABLE_BASE_ID = "apppMIezgkBvybCwx"
AIRTABLE_TABLE_ID = "tblvfmM6U96Om3tAZ"
AIRTABLE_VIEW_ID = "viw5ezbzfDCv7rDrP"

GDRIVE_FOLDER_ID = "1R_ZwcBI1D1q86jYx-IQCiAOYFcvmXnzp"

# Google service account credentials JSON - paste the full JSON as an env var
# e.g. GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]


def fetch_airtable_records():
    api = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)
    records = table.all(view=AIRTABLE_VIEW_ID)
    return records


def records_to_csv(records):
    if not records:
        raise ValueError("No records returned from Airtable")

    # Collect all field names across all records (some may be sparse)
    all_fields = []
    seen = set()
    for record in records:
        for key in record["fields"].keys():
            if key not in seen:
                all_fields.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_fields, extrasaction="ignore")
    writer.writeheader()
    for record in records:
        writer.writerow(record["fields"])

    return output.getvalue()


def upload_to_gdrive(csv_content, filename):
    import json

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": filename,
        "parents": [GDRIVE_FOLDER_ID]
    }

    media = MediaIoBaseUpload(
        io.BytesIO(csv_content.encode("utf-8")),
        mimetype="text/csv",
        resumable=False
    )

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name, webViewLink"
    ).execute()

    return file


def generate_filename():
    # Format: Amondo-YY-MM-DD.csv
    now = datetime.utcnow()
    return f"Amondo-{now.strftime('%y-%m-%d')}.csv"


def main():
    print("Fetching records from Airtable...")
    records = fetch_airtable_records()
    print(f"Got {len(records)} records")

    print("Converting to CSV...")
    csv_content = records_to_csv(records)

    filename = generate_filename()
    print(f"Uploading {filename} to Google Drive...")
    result = upload_to_gdrive(csv_content, filename)

    print(f"Done! File uploaded: {result['name']}")
    print(f"View it here: {result['webViewLink']}")


if __name__ == "__main__":
    main()

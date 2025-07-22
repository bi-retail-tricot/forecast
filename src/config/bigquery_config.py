from dotenv import load_dotenv
import os
from google.oauth2 import service_account

load_dotenv()

SERVICE_ACCOUNT_GBQ = os.getenv("SERVICE_ACCOUNT_GBQ")
PROJECT_ID_GBQ = os.getenv("PROJECT_ID_GBQ")

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform'
]

CREDENTIALS_GBQ = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_GBQ,
    scopes=SCOPES
)

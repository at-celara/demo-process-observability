from __future__ import annotations

from pathlib import Path
from typing import Any


def make_gmail_service(credentials_json: Path, subject_email: str):
    """
    Create a Gmail API service client using a service account with domain-wide delegation.
    Scope: gmail.readonly
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception as e:
        raise RuntimeError(
            "google-api-python-client and google-auth are required for Gmail ingestion"
        ) from e

    creds = service_account.Credentials.from_service_account_file(
        str(credentials_json),
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
    )
    delegated = creds.with_subject(subject_email)
    service = build("gmail", "v1", credentials=delegated, cache_discovery=False)
    return service

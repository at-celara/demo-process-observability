from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class NormalizedMessage(BaseModel):
    # Required fields
    message_id: str
    source: str
    timestamp: str
    text: str
    thread_id: Optional[str] = None

    # Recommended optional fields
    sender: Optional[str] = None
    sender_name: Optional[str] = None
    recipients: Optional[List[str]] = None
    subject: Optional[str] = None
    has_attachments: Optional[bool] = None
    account_email: Optional[str] = None
    attachments: Optional[List[Dict[str, Any]]] = None

    # Raw preservation
    raw: Optional[Dict[str, Any]] = None

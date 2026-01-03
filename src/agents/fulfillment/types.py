"""
Fulfillment Agent - Type Definitions and Models
"""
from typing import Optional, List, Dict
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class FulfillmentState(str, Enum):
    """Fulfillment Agent states"""
    INTRO = "FULFILL_INTRO"
    LIST = "FULFILL_LIST"
    WAIT_PICK = "FULFILL_WAIT_PICK"
    NOMINATE = "FULFILL_NOMINATE"
    DONE = "FULFILL_DONE"
    EXIT = "FULFILL_EXIT"


@dataclass
class NeedCard:
    """Structure for a need/opportunity card"""
    need_id: str
    title: str
    org_name: str
    location: str
    days_text: Optional[str] = None  # e.g., "Mon & Wed"
    time_text: Optional[str] = None  # e.g., "3:30–4:30 PM IST"
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "need_id": self.need_id,
            "title": self.title,
            "org_name": self.org_name,
            "location": self.location,
            "days_text": self.days_text,
            "time_text": self.time_text,
        }


# TODO: Define nomination model (for future use)
# class Nomination:
#     nomination_id: str
#     need_id: str
#     volunteer_phone: str
#     volunteer_name: str
#     volunteer_email: str
#     status: str  # "pending", "accepted", "rejected"
#     nominated_at: datetime
#     confirmed_at: Optional[datetime]

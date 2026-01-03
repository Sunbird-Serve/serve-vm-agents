"""
Selection Agent - Type Definitions and Models
"""
from typing import Optional, Literal
from enum import Enum


class SelectionState(str, Enum):
    """Selection Agent states"""
    START = "SEL_START"
    WAIT_VIDEO_DONE = "SEL_WAIT_VIDEO_DONE"
    KNOWING_VOLUNTEER_LOOP = "SEL_KNOWING_VOLUNTEER_LOOP"
    EVALUATE = "SEL_EVALUATE"
    RECOMMENDED = "SEL_RECOMMENDED"
    NOT_RECOMMENDED = "SEL_NOT_RECOMMENDED"
    STOP = "SEL_STOP"


class RecommendationOutcome(str, Enum):
    """Recommendation decision outcomes"""
    RECOMMENDED = "recommended"
    NOT_RECOMMENDED = "not_recommended"
    PENDING = "pending"

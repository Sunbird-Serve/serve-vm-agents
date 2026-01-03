"""
Selection Agent - Prompts and Message Templates
"""
import os
from .config import settings

# Get video URL from env or use fallback
WELCOME_VIDEO_URL = os.getenv("WELCOME_VIDEO_URL", settings.WELCOME_VIDEO_URL)


def get_sel_video_intro(name: str = "there") -> str:
    """Get video intro message with name"""
    return f"Before we continue, I have something special for you, {name} — a short welcome video"


def get_sel_video_done_prompt() -> str:
    """Get video done prompt"""
    return "When you're done watching, just reply *Done*"


def get_sel_video_followup() -> str:
    """Get video followup message"""
    return "Hope you liked it! We wanted you to feel the warmth and appreciation that every SERVE volunteer deserves"


def get_sel_about_you(name: str = "there") -> str:
    """Get about you question with name"""
    return f"Let's start with something simple, {name}. Tell me a little about yourself — anything you'd like to share"


# Recommended message
SEL_RECOMMENDED_MSG = "You're all set, Let's look at open opportunities."

# Not recommended message (placeholder, not used now)
SEL_NOT_RECOMMENDED_MSG = """Thank you for your interest. We're currently looking for volunteers with specific qualifications.

You're always welcome to stay connected with the SERVE community and explore other ways to contribute.

👉 Join the SERVE Community: https://serve.sunbird.org/community

Wishing you a lovely day 🌼"""

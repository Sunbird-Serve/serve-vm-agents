"""
Selection Agent - Prompts and Message Templates
"""
import os
from .config import settings

# Get video URL from env or use fallback
WELCOME_VIDEO_URL = os.getenv("WELCOME_VIDEO_URL", settings.WELCOME_VIDEO_URL)


def get_sel_video_intro(name: str = "there") -> str:
    """Get video intro message with name"""
    return f"Here's a quick note from our team, {name} - we're in the last stretch now 🙂"


def get_sel_video_done_prompt() -> str:
    """Get video done prompt"""
    return "Reply Done when you're ready.."


def get_sel_video_followup(name: str = "there") -> str:
    """Get video followup message"""
    return f"Thanks for staying with me, {name} 🙂 This is the last part — just a few things to get to know you better."


def get_sel_about_you(name: str = "there") -> str:
    """Get about you question with name"""
    return f"Let's start with something simple, {name}. Tell me a little about yourself — anything you'd like to share"


def get_sel_language_comfort_prompt(language: str, name: str = "there") -> str:
    """Get language comfort prompt with buttons"""
    return (
        f"Quick one, {name} — for {language}, which option fits you best?\n"
        "Read, Write, Speak, or All"
    )


SEL_DEFERRED_MSG = """No worries — take your time.
Whenever you're ready, just reply here and we’ll pick up from where we left off."""


# Recommended message
def get_sel_recommended_msg(name: str = "there") -> str:
    """Get recommended message with name"""
    return f"Thanks {name} for sharing! You're all set, Let's look at open opportunities."

# Not recommended message (placeholder, not used now)
SEL_NOT_RECOMMENDED_MSG = """Thank you for your interest. We're currently looking for volunteers with specific qualifications.

You're always welcome to stay connected with the SERVE community and explore other ways to contribute.

👉 Join the SERVE Community: https://serve.sunbird.org/community

Wishing you a lovely day 🌼"""

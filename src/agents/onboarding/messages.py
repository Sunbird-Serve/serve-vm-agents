"""
Message Templates for Onboarding Agent

All conversational messages are defined here for easy modification and consistency.
"""

# ---------- Accepted Response Variations ----------
YES_WORDS = {"yes", "y", "ok", "okay", "sure", "go ahead", "continue", "proceed", 
             "ya", "haan", "start", "begin", "let's start", "lets start", 
             "correct", "right", "yup", "yep", "that's right", "thats right",
             "perfect", "exactly", "absolutely", "definitely", "of course",
             "sounds good", "sounds great", "good", "great", "fine", "alright",
             "sure thing", "why not", "i agree", "agreed", "true", "indeed",
             "should be fine", "i think so", "seems fine", "works", "works for me",
             "fine by me"}

NO_WORDS = {"no", "n", "nope", "nah", "not really", "no thanks", "not interested",
            "don't want", "dont want", "not now", "pass", "skip", "decline"}

MAYBE_LATER = {"later", "maybe later", "not now", "remind me", "maybe", "perhaps"}

CONFIRM_WORDS = {"yes", "correct", "right", "yup", "yep", "confirm", "confirmed", 
                 "that's right", "thats right", "perfect", "exactly", "ok", "okay",
                 "absolutely", "definitely", "of course", "sounds good", "sounds great",
                 "good", "great", "fine", "alright", "sure", "sure thing", "agreed",
                 "true", "indeed", "precisely", "spot on", "bingo", "exactly right"}

EDIT_WORDS = {"edit", "no", "wrong", "change", "incorrect", "not right", "not correct",
              "modify", "update", "fix", "redo", "start over", "try again"}


# ---------- Section 1: Welcome & Consent ----------
WELCOME = """Hey {name}!
Welcome to the SERVE Volunteer Program — where caring people like you help rural children learn through live online classes.
Shall we get you started?"""

WELCOME_MAYBE_LATER = """No worries! You can come back anytime using this same link.

Just send 'start' whenever you're ready."""


# ---------- Section 2: Eligibility Screening ----------
ELIGIBILITY_INTRO = """Lovely!
Quick one — you're 18 or older, right?"""

ELIGIBILITY_Q1 = """Perfect! And you have a smartphone or laptop with internet?"""

ELIGIBILITY_Q2 = """Great — that's all we need. Most volunteers give about 2 hours a week; comfortable for you?"""

ELIGIBILITY_Q3 = """Awesome!"""

ELIGIBILITY_INVALID_RESPONSE = """Please reply with 'Yes' or 'No'."""

REJECTED = """Thanks for your interest!

Right now, the teaching program needs volunteers who can meet these 3 requirements.

We will keep you posted on other ways to contribute."""

ELIGIBILITY_PASSED = """Awesome!"""

# Gentle persuasion when slightly below commitment
PERSUADE_COMMITMENT = """Totally get it. Many volunteers start small and adjust.
If you can try around 2 hours a week for about 3 months, it really helps the kids build a habit.
Would giving it a try be okay? (Yes/No)"""


# ---------- Section 3: Teaching Preferences ----------
ASK_TEACHING_PREF = """What would you love to teach? You can type it in one line, like "Math for Grade 6 in Hindi." """

CONFIRM_TEACHING_PREF = """Nice! So {subjects}, {grades}, {language} — did I get that right?"""

EDIT_TEACHING_PREF = """No problem! Please share your teaching preferences again.

Example: "Math for Grade 6 in Hindi" """

TEACHING_PREF_UNCLEAR = """I couldn't quite understand that. Could you try again?

Please mention:
- Subject(s): Math, English, Science, etc.
- Grade(s): 6-8, 9-10, 11-12, etc.
- Language: Hindi, English, Tamil, Kannada, Telugu, or Other

Example: "Math for Grade 8 in Tamil" """


# ---------- Section 4: Orientation Scheduling ----------
ASK_AVAILABILITY = """Perfect.
Before your first class, we'll do a short orientation (about 30 mins).
Could you share 2–3 time slots that work for you? (e.g., "Tue 12:30–1:00 PM" or "Thu 2:30–3:00 PM")"""

# Announce class timing constraints and ask for consent (not orientation)
CONSTRAINTS_ANNOUNCE = """Before we proceed: our live classes run only on weekdays, between 8 AM and 3 PM.
Does that work for you? (Yes/No)"""

AVAILABILITY_PARSE_FAILED = """I couldn't quite understand those times.

Could you share your available times again?
(e.g., 'tomorrow 12:30', 'Tuesday 2:30 PM', 'Saturday 10 AM')"""

CONFIRM_SLOT_TEMPLATE = """Thanks! I found these slots:
{slot_options}

Please pick one by saying:
• The day/time (e.g., "Tuesday 12:30 PM")
• The number (e.g., "1" for first option, "2" for second)
• Or "Yes" for the first option

Which one works for you?"""

CONFIRM_SLOT_INVALID = """I didn't quite catch that. Could you please:
• Pick one of the slots above (e.g., "Friday 9 PM")
• Say a number (1, 2, etc.)
• Or say "Yes" for the first option"""

SLOT_NONE_OF_ABOVE = """No problem! Those times don't work for you.
Please share 2-3 different time options that work for you.
(e.g., 'tomorrow 12:30', 'Tuesday 2:30 PM', 'Saturday 10 AM')"""

CONFIRM_BOOKING = """Perfect — locking that in. One sec..."""

CONFIRM_BOOKING_INVALID = """Please reply to confirm, or let me know if you want a different slot."""

BOOKING_IN_PROGRESS = """All set!"""

# Gentle persuasion for weekend-only responses
PERSUADE_WEEKEND_ONLY = """I understand weekends are easier. If you can find a small weekday slot (even 20–30 minutes, like a lunch break), it helps us align with school schedules and reach more students.
Could you share any weekday times between 8 AM and 3 PM that might work?"""


# ---------- Section 5: Final Confirmation ----------
DONE = """Orientation: {slot_label}
Meet link: {meet_link}

You're officially part of the SERVE Volunteer Community, {name}!
Every hour you share helps a child learn better. See you at the orientation!"""


# ---------- General Messages ----------
ALREADY_DONE = """We've captured your details. Type 'restart' to start over."""

RESTARTING = """Restarting your onboarding. Let's begin fresh!"""


# ---------- Helper Functions ----------
def format_message(template: str, **kwargs) -> str:
    """
    Format a message template with provided data
    
    Args:
        template: Message template string
        **kwargs: Data to fill into template
        
    Returns:
        Formatted message string
    """
    # Provide defaults for optional fields
    defaults = {
        "name": "Volunteer",
        "subjects": "N/A",
        "grades": "N/A",
        "language": "N/A",
        "slot_label": "TBD",
        "meet_link": "Will be sent soon"
    }
    
    # Merge defaults with provided kwargs
    data = {**defaults, **kwargs}
    
    return template.format(**data)


def format_slot_options(slots: list[dict]) -> str:
    """
    Format slot options for display in a natural conversational way
    
    Args:
        slots: List of slot dicts with 'label' key
        
    Returns:
        Formatted string like "Thu 16 Oct 1 PM or Sat 18 Oct 6 PM"
    """
    if not slots:
        return "the times you mentioned"
    
    labels = [slot.get('label', 'Unknown time') for slot in slots[:3]]  # Max 3 slots
    
    if len(labels) == 1:
        return labels[0]
    elif len(labels) == 2:
        return f"{labels[0]} or {labels[1]}"
    else:
        # Three slots: "A, B, or C"
        return f"{labels[0]}, {labels[1]}, or {labels[2]}"


def format_subjects_list(subjects: list[str]) -> str:
    """Format list of subjects as comma-separated string"""
    if not subjects:
        return "N/A"
    return ", ".join(subjects)


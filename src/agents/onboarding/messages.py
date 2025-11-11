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

WELCOME_INTRO = """Hey {name}! I'm Sia from the SERVE team 😊

Thank you for registering with us — it's wonderful to have you here.

If it's okay with you, I'll quickly walk you through how SERVE works and get you onboarded. Shall we continue?"""

WELCOME_SERVE_OVERVIEW = """Beautiful 🌼

SERVE connects volunteers like you with students in government and rural schools. You teach online, students join from their classroom.

This is a volunteer opportunity (not a paid role), but your time directly strengthens children’s learning.

Shall I take you through a few quick questions to get you ready?"""

WELCOME_CONSENT_ACK = """Great, I'll keep it simple and quick."""

WELCOME_CONSENT_REMINDER = """Quick reminder — shall we continue with the onboarding?"""


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

ELIGIBILITY_AGE_PROMPT = """First, to make sure you meet our policy — may I check if you're 18 or older?"""

ELIGIBILITY_AGE_UNCLEAR = """I didn't quite catch that. Could you confirm: are you 18 or older? (Yes/No)"""

ELIGIBILITY_UNDERAGE_DECLINE = """Thanks for your enthusiasm 💛
At the moment, SERVE is open to adults (18+). Once you turn 18, I'll be happy to help you start your volunteering journey. 🌟"""

ELIGIBILITY_DEVICE_PROMPT = """Thank you! Do you have a smartphone or laptop and a stable internet connection to join the live classes?"""

ELIGIBILITY_DEVICE_CLARIFY = """Could you clarify, {name}? Do you have a smartphone or laptop with internet access?"""

ELIGIBILITY_DEVICE_DEFERRAL = """No worries, {name} 😊 You'll just need a phone/laptop and steady internet to teach.
I can check back once you have access — would you like me to remind you next week?"""

ELIGIBILITY_DEVICE_DEFERRAL_CONFIRM = """Perfect! I'll remind you next week, {name}. Feel free to ping me anytime when you're ready."""

ELIGIBILITY_DEVICE_DEFERRAL_FALLBACK = """Got it, {name}! I'll check back with you later when you're ready."""

ELIGIBILITY_DEVICE_REASK = """No problem. Could you confirm if you have a smartphone or laptop with internet?"""

ELIGIBILITY_DEVICE_OK = """Great! 👍"""

ELIGIBILITY_COMMIT_PROMPT = """And would you be able to contribute around 2 hours a week for teaching? We're flexible with days, but a minimum of 2 hours weekly is needed to keep classes consistent for the children.
"""

ELIGIBILITY_COMMIT_CLARIFY = """Could you confirm: can you spare around 2 hours per week for teaching?"""

ELIGIBILITY_COMMIT_POLICY = """Thanks for checking. The 2 hours need to be split across different weekdays during school hours (8–15), not all on the same day. Would two short weekday slots work for you—say 30–45 minutes each on different days?"""

ELIGIBILITY_COMMIT_SUCCESS = """Awesome! 🎉"""

ELIGIBILITY_PREFERENCES_PROMPT = """What days usually work for you to take class?
You can pick 2–3 days (e.g., Mon, Wed, Sat) — or just type what suits you."""

ELIGIBILITY_PREFERENCES_WEEKEND_NOTE = """\n\nNote: Weekends are reserved — weekdays are best."""

ELIGIBILITY_COMMIT_PERSUADE = """I understand, {name} 😊 Even 2 hours a week can make a big difference for the children — and you can pick times that suit you!
Do you think that might work?"""

ELIGIBILITY_COMMIT_DEFERRAL = """Totally fine, {name} 💛 I'll note that you'd like to start later and remind you in a few days."""

ELIGIBILITY_COMMIT_DEFERRAL_CONFIRM = """Perfect! I'll remind you next week, {name}. Feel free to ping me anytime when you're ready."""

ELIGIBILITY_DECLINE_REQUIREMENTS = """Thanks for your interest! Right now, the teaching program needs volunteers who can meet these requirements. We will keep you posted on other ways to contribute."""

ELIGIBILITY_DECLINE_GENERIC = """Thanks for your interest! Right now, we need volunteers who meet all requirements."""


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


# ---------- Section 4: Teaching Preferences (continued) ----------
PREFS_PROMPT = """What days usually work for you to take class?
You can pick 2–3 days (e.g., Mon, Wed, Sat) — or just type what suits you."""

PREFS_WEEKEND_NOTE = """\n\nNote: Weekends are reserved — weekdays are best."""

PREFS_COMBINED_CLARIFIER = """Could you pick 2–3 days (e.g., Mon, Wed) and a time band — Morning (8–11) or Afternoon (12–4)?"""

PREFS_ASK_TIME = """Great. Do mornings (8AM –11AM) or afternoons (12PM–3PM) work best in IST?"""

PREFS_ASK_DAYS = """Could you pick two weekdays (Mon–Fri)?"""

PREFS_CONFIRM_WITH_WEEKEND = """I’ve noted {days}, though weekends are limited in your region. I’ll prioritize weekdays in {band} IST. 👍"""

PREFS_CONFIRM_DEFAULT = """Noted 👍 I’ll look for {days} in {band} IST."""

PREFS_SAVE_FALLBACK_NO_DAYS = """Could you pick two weekdays (Mon–Fri)?"""

PREFS_SAVE_FALLBACK_NO_TIME = """Do mornings (8–11), afternoons (12–4), or evenings (5–8) work best?"""

PREFS_EVENING_POLICY = (
    "Most of our school sessions run between 8 AM and 4 PM IST. "
    "If you can manage any slot in the mornings, lunch break, or early afternoon on weekdays, "
    "we can match you much faster 👍"
)

PREFS_EVENING_DEFERRAL = (
    "Totally understand evenings are easiest. Our current school sessions all run before 4 PM, "
    "so I'll note your interest for future evening opportunities. "
    "Feel free to ping me anytime if a weekday slot opens up for you!"
)


# ---------- Section 5: QA Window & Orientation Transition ----------
QA_ENTRY_PROMPT = """Before we wrap up, do you have any quick questions for me? (training, certificate, subjects, tech setup…)

I'll keep it short and clear. 🙂"""

QA_MANDATORY_ORIENT = """One last step: a short orientation (about 30 minutes) is mandatory for all volunteers.

It helps you understand the platform, classroom flow, and support available so you feel fully ready.

Please share 2–3 slots that work for you in the next few days (e.g., "Sat 4 PM" or "Sun 10 AM")."""

QA_CONTINUE_PROMPT = QA_MANDATORY_ORIENT

QA_NUDGE = QA_MANDATORY_ORIENT

QA_DEFERRAL_PROMPT = """No worries 😊 I can check back later. When should I remind you — Tue 10am, Thu 6pm, or Sat 10am?"""

QA_STOP_ACK = """Understood. I'll stop messages. If you change your mind, just say "Hi" here anytime. 💛"""

QA_FAQ_ABOUT_SERVE = """SERVE helps thousands of children learn English, Science, and Maths through volunteers like you. You teach online — they learn in school — and our local coordinators make sure everything runs smoothly."""

QA_FAQ_TIME_PROCESS = """You'll teach live online while students sit in their school smart classroom.
Usually ~2 hours/week."""

QA_FAQ_SUPPORT = """Yes! You'll attend a 30-min online orientation, and a local coordinator supports you during classes."""

QA_FAQ_CERTIFICATE = """We provide a volunteer certificate after you complete the required sessions as per policy."""

QA_FAQ_SUBJECTS_GRADES = """Most volunteers teach English, Math or Science for grades 5–8 (varies by school).
We'll align your preferences during scheduling."""

QA_FAQ_TECH = """A phone or laptop with stable internet is enough. We'll share the Meet link for sessions."""

# ---------- Section 6: Orientation Scheduling ----------
ASK_AVAILABILITY = """One last step: a short orientation (about 30 minutes) is mandatory for all volunteers.

It helps you understand the platform, classroom flow, and support available so you feel fully ready.

Please share 2–3 slots that work for you in the next few days (e.g., "Sat 4 PM" or "Sun 10 AM")."""

ORIENT_INTRO = """One last step: a short 30-minute online orientation so you feel fully ready.
It covers how classes work, tech setup, and support. 😊

Please share 2–3 time slots in the next few days that work for you (e.g., "Sat 4–4:30 PM" or "Sun 10–10:30 AM")."""

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

ORIENT_SHOW_OPTIONS = """Here are a couple of options based on your availability:
{options}

Please reply with the number or the day & time."""

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

# Orientation-specific messaging
ORIENT_AVAILABILITY_ACK = """Got it! I’ll propose a couple of options that fit those times."""

ORIENT_PROPOSAL_INTRO = """Here are the options based on your preference:
{options}
Please reply with the option number or the day/time."""

ORIENT_PROPOSAL_NO_SLOTS = """Sorry, I couldn't find available slots right now. Could you try again with different times?"""

ORIENT_PROPOSAL_ERROR = """There was an issue proposing slots. Could you try again with different times?"""

ORIENT_INVALID_SELECTION = """Please reply with 1 or 2, or type the day/time."""

ORIENT_SLOT_UNAVAILABLE = """Sorry, that slot is no longer available. Please pick another one."""

ORIENT_BOOKING_CONFIRM = """Perfect — locking that in. One sec..."""

ORIENT_BOOKING_FAILURE = """Sorry, there was an error booking your slot. Please try again or contact support."""

ORIENT_CONFIRM = """Perfect ✅
Orientation: {slot}
Link: {meet_link}
You'll get a reminder before the session. Welcome to the SERVE Volunteer Community 💛"""

ORIENT_INVALID_PICK = """I couldn’t match that to the available slots 🙈 Could you please reply with one of the shown options (like 1 or 2), or type the exact day & time?"""

ORIENT_LATER_NOTE = """No worries 😊 You can message me here anytime to fix your orientation. We’ll keep your details ready."""


# ---------- Section 5: Final Confirmation ----------
DONE = """Orientation: {slot_label}
Meet link: {meet_link}

Welcome to the SERVE Volunteer Community, {name}!
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


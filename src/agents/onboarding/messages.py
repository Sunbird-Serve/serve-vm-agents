# -*- coding: utf-8 -*-
"""
Message Templates for Onboarding Agent

All conversational messages are defined here for easy modification and consistency.
"""

# ---------- Accepted Response Variations ----------
YES_WORDS = {"yes", "y", "ok", "okay", "sure", "go ahead", "continue", "proceed", 
             "ya", "haan", "start", "begin", "let's start", "lets start", 
             "correct", "right", "yup", "yep", "that's right", "thats right",
             "perfect", "exactly", "absolutely", "definitely", "of course","ofcourse","of course yes","ofcourse yes",
             "sounds good", "sounds great", "good", "great", "fine", "alright","yes ofcourse","yes of course",
             "sure thing", "why not", "i agree", "agreed", "true", "indeed",
             "should be fine", "i think so", "seems fine", "works", "works for me",
             "fine by me","yes please","please proceed","Ofcourse","Ofcource yes"}

NO_WORDS = {"no", "n", "nope", "nah", "not really", "no thanks", "not interested",
            "don't want", "dont want", "not now", "pass", "skip", "decline"}

MAYBE_LATER = {"later", "maybe later", "not now", "remind me", "maybe", "perhaps"}

CONFIRM_WORDS = {"yes", "correct", "right", "yup", "yep", "confirm", "confirmed", 
                 "that's right", "thats right", "perfect", "exactly", "ok", "okay",
                 "absolutely", "definitely", "of course", "sounds good", "sounds great",
                 "good", "great", "fine", "alright", "sure", "sure thing", "agreed",
                 "true", "indeed", "precisely", "spot on", "bingo", "exactly right", "of course","ofcourse","of course yes","ofcourse yes"}

EDIT_WORDS = {"edit", "no", "wrong", "change", "incorrect", "not right", "not correct",
              "modify", "update", "fix", "redo", "start over", "try again"}


# ---------- Section 1: Welcome & Consent ----------
WELCOME = """Hey {name}!
Welcome to the SERVE Volunteer Program — where caring people like you help rural children learn through live online classes.
Shall we get you started?"""

WELCOME_MAYBE_LATER = """No worries! You can come back anytime using this same link.

Just send 'start' whenever you're ready."""

WELCOME_INTRO = """Hi! I'm SIA - I represent eVidyaloka’s Volunteering Team.

Thank you for being here for the cause of children’s education in government schools, supported through live online teaching.
It’s my pleasure to welcome you on this journey 🌱"""

WELCOME_INSTRUCTIONS = """I’m here to guide you through getting started as a volunteer —
just three simple parts 🙂

🌼 Say Hello - a warm welcome and a quick peek into real classrooms
🌱 Find Your Rhythm - figuring out what works for your time and comfort
🌿 Know You Better - a short, friendly chat about you

About 10 minutes, at your pace.
You can pause anytime."""

WELCOME_START_BUTTONS = ["Let's start", "I'll do this later"]

WELCOME_VIDEO_INTRO = "Here’s a quick hello from our team 🙂"
WELCOME_VIDEO_FOOTER = "When you’re done watching, just reply Done."

GENERIC_DEFERRED_MSG = """No worries — take your time.
Whenever you're ready, just reply here and we'll continue from where you left off."""

WELCOME_SERVE_OVERVIEW = """Beautiful 🌼

SERVE connects volunteers like you with students in government and rural schools. You teach online, students join from their classroom.

This is a volunteer opportunity (not a paid role), but your time directly strengthens children's learning.

Shall I take you through a few quick questions to get you ready?"""

WELCOME_CONSENT_ACK = """Great, I'll keep it simple and quick."""

WELCOME_CONSENT_REMINDER = """Quick reminder — shall we continue with the onboarding?"""


# ---------- Section 2: Intent (Purpose Acknowledgement) ----------
INTENT_PROMPT = """Before we continue, I'm just curious - What made you explore this today? 🙂"""

INTENT_FOLLOWUP = """If you'd like, I can quickly walk you through what volunteering with SERVE looks like."""

INTENT_EXIT = """I understand 💛  

You're always welcome to stay connected with the SERVE community  

and explore volunteering or contribution opportunities in the future.

👉 Join SERVE Community: https://serve.sunbird.org/community  

Thank you for your interest, and wishing you a lovely day 🌼"""


# ---------- Section 2.1: Readiness Check ----------
READINESS_CHECK_PROMPT = """Before we dive in, a quick check 🙂 
Would you like to continue learning about volunteering with SERVE now, or come back later?"""

READINESS_CHECK_BUTTONS = ["Continue Now", "Later works better"]

READINESS_DEFERRED_MSG = """No worries at all 😊
Come back whenever it works for you — I'll be right here."""


# ---------- Section 2.2: Intent (Purpose Acknowledgement) ----------
# INTENT_PROMPT already defined above
INTENT_ACKNOWLEDGEMENT = """Nice 🙂"""


# ---------- Section 2.3: Class Preview Ask ----------
CLASS_PREVIEW_ASK_PROMPT = """BTW! Would you like to see a short glimpse of how a SERVE class usually looks?"""

CLASS_PREVIEW_ASK_BUTTONS = ["Yes, show me", "Skip for now"]


# ---------- Section 2.4: Video ----------
VIDEO_INTRO = "Here's a short glimpse of how a class usually looks"

VIDEO_FOOTER = "Reply Done when ready"

VIDEO_DONE_PROMPT = "Thanks for watching 🙂"

VIDEO_ERROR_MSG = """I'm having trouble playing the video right now, but we can still continue."""

PEEK_VIDEO_PROMPT = "By the way, would you like to watch a short live class glimpse? You can say yes, skip, or maybe."

PEEK_NEEDS_PROMPT = "Would you like to see a quick preview of current requirements? You can say yes, skip, or maybe."
PEEK_REQUIREMENTS_NOTE = "This is just a preview — we’ll later look at what works for you."
PEEK_SKIP_MESSAGE = "From here, we’ll move into a few basics — about 10 minutes, at your pace 🙂"


# ---------- Section 2.5: Needs Preview ----------
NEEDS_PREVIEW_HEADER = """To give you a sense of how volunteering usually looks, here are a couple of current examples 👇"""

NEEDS_PREVIEW_DISCLAIMER = """This is just a preview — Later, we'll understand your comfort and availability and match you thoughtfully. """

NEEDS_PREVIEW_ERROR_MSG = """I'm having trouble fetching the latest list right now — but we can still continue."""


# ---------- Section 2.8: Continue Confirm (Time Expectation) ----------
CONTINUE_CONFIRM_PROMPT = """From here, it'll just take about 10 minutes to check a few basics and see available classes 😊

Shall we continue?"""

CONTINUE_CONFIRM_BUTTONS = ["Yes, continue", "I'll come back later"]

CONTINUE_CONFIRM_DEFERRED_MSG = """No worries at all 😊
You can message here whenever you're ready."""


# ---------- Section 3: Eligibility Check ----------
# Commitment check (first step in eligibility)
ELIGIBILITY_COMMITMENT_PROMPT = """Before we go ahead, just to make sure this works smoothly for you and the students —  

would you be comfortable teaching around *2 hours a week*?  

(We always try to work around your routine 😊)"""

ELIGIBILITY_COMMITMENT_PERSUASION = """That's completely okay — even small amounts of time can make a big difference. 🌱  

Many volunteers try one session on Saturday and another during the week."""

ELIGIBILITY_PROMPT = """Just so you can decide if this feels right for you  

Volunteering with SERVE usually means:

* teaching students online using laptop or a tablet (not phone)  

* spending around *2 hours a week*

* contributing in a *voluntary (unpaid)* role

* are 18 years or above

If this sounds like something you'd enjoy, we can continue"""

# Button labels for ELIGIBILITY main prompt
ELIGIBILITY_BUTTONS = ["Yes, this works", "Tell me more", "Something won't work"]

# Button labels for issue selection
ELIGIBILITY_ISSUE_SELECTION_BUTTONS = ["Age", "Device", "Time", "Unpaid", "Other"]

# Button labels for issue-specific confirmations
ELIGIBILITY_ISSUE_AGE_BUTTONS = ["Yes", "No"]
ELIGIBILITY_ISSUE_DEVICE_BUTTONS = ["Yes", "No"]
ELIGIBILITY_ISSUE_TIME_BUTTONS = ["Yes", "No"]
ELIGIBILITY_ISSUE_UNPAID_BUTTONS = ["Yes", "No"]

# Message for "Tell me more"
ELIGIBILITY_TELL_ME_MORE_MSG = """Of course 🙂 We partner with government schools for live, interactive teaching.
We align on these basics so the experience is safe, meaningful, and smooth for both you and students."""

# Message for issue selection
ELIGIBILITY_ISSUE_SELECTION_MSG = """Thanks for telling me 💛 Which part feels difficult right now?"""

# Issue-specific prompts
ELIGIBILITY_ISSUE_AGE_PROMPT = """Just to confirm — are you *18 or above*?"""

ELIGIBILITY_ISSUE_DEVICE_PROMPT = """Do you have access to a *laptop or tablet with internet* for sessions? (Phones won't work well for teaching.)"""

ELIGIBILITY_ISSUE_TIME_PROMPT = """Would around *2 hours a week* generally work for you?"""

ELIGIBILITY_ISSUE_UNPAID_PROMPT = """Are you comfortable with this being a *voluntary (unpaid)* role?"""

ELIGIBILITY_ISSUE_OTHER_PROMPT = """Tell me what feels difficult right now, and I'll guide you 🙂"""

# Progressive clarification prompts (with buttons)
ELIGIBILITY_CLARIFY_AGE_PROMPT = """Just to confirm — are you **18 or above**?"""
ELIGIBILITY_CLARIFY_AGE_BUTTONS = ["Yes", "No"]

ELIGIBILITY_CLARIFY_DEVICE_PROMPT = """Do you have a **laptop or tablet** with a stable internet connection?

(Phones aren't suitable for live classes)"""
ELIGIBILITY_CLARIFY_DEVICE_BUTTONS = ["Yes", "No"]

ELIGIBILITY_CLARIFY_UNPAID_PROMPT = """And are you okay with this being a **voluntary (unpaid)** role?"""
ELIGIBILITY_CLARIFY_UNPAID_BUTTONS = ["Yes", "No"]

ELIGIBILITY_EXIT = """I really appreciate you taking the time to share that with me 💛

These requirements help ensure students get consistent, quality support during their live classes. That's why we need 18+, a tablet or laptop with internet, and understanding it's a volunteer role.

I know this might be disappointing, and I'm sorry we can't move forward right now.

The good news? You're always welcome in the SERVE community — there are other meaningful ways to contribute and stay connected.

👉 Join us: https://serve.sunbird.org/community

If you feel this might work for you later, you can message me here anytime 💛

Thank you for your interest, and wishing you a lovely day 🌼"""


# ---------- Section 4: Identity Collection ----------
IDENTITY_NAME_PROMPT = """Lovely!  

May I know your **name**? 🙂"""

IDENTITY_CONTACT_PROMPT = """Thanks {name}!  

Could you share your **email**?  

This helps create your volunteer profile and keep you updated on sessions."""

IDENTITY_CONTACT_RETRY = """Could you share your email?"""

IDENTITY_CONFIRM_CONTACT = """Got it! 

Phone: **{phone}**  
Email: **{email}**  

Is this correct? Reply Yes or No"""

IDENTITY_EMAIL_CORRECTION = """No worries! Could you share your email again? 🙂"""

IDENTITY_NAME_RECHECK = """Just checking — what name should I use to address you?"""

IDENTITY_REGISTRATION_START = """Perfect, thanks! I'm just creating your SERVE profile — this may take a few seconds. You don't need to type anything, I'll let you know once it's done ✅"""

IDENTITY_REGISTRATION_EXISTING = """Looks like you already have a SERVE profile — I'll continue from there ✅"""

IDENTITY_REGISTRATION_CREATED = """Done! Your SERVE volunteer profile is created ✅"""

IDENTITY_REGISTRATION_FAILED = """I hit a small issue creating your profile. A coordinator will reach out. Let's continue"""

IDENTITY_REGISTRATION_WAIT_REASSURANCE = """I'm still working on your profile — almost done, thank you for waiting 💛"""

IDENTITY_NUDGE = """I understand your concern 💛  

These details are important so we can  
• create your volunteer profile  
• coordinate sessions  
• and support you properly once you start"""

IDENTITY_BOUNDARY = """Without these details, I won't be able to move you into classroom volunteering just yet."""

IDENTITY_EXIT = """You're very welcome to stay connected with the SERVE community,  
learn more about our work, and explore other ways to contribute.

👉 Join SERVE Community: https://serve.sunbird.org/community  

Thank you for your interest, {name}.  
Hope to see you again 🌱"""


# ---------- Section 5: Eligibility Screening (Old - kept for reference) ----------
ELIGIBILITY_INTRO = """Lovely!
Quick one — you're 18 or older, right?"""

ELIGIBILITY_Q1 = """Perfect! Since the classes are live online, do you have a smartphone or laptop with a fairly stable internet connection?"""

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

ELIGIBILITY_AGE_ACK = """Awesome! Thanks for confirming, {name}! 😊"""

ELIGIBILITY_DEVICE_PROMPT = """Since the classes are live online, do you have a smartphone or laptop with a fairly stable internet connection?"""

ELIGIBILITY_DEVICE_CLARIFY = """Could you clarify, {name}? Do you have a smartphone or laptop with internet access?"""

ELIGIBILITY_DEVICE_DEFERRAL = """No worries, {name} 😊 You'll just need a phone/laptop and steady internet to teach.
I can check back once you have access — would you like me to remind you next week?"""

ELIGIBILITY_DEVICE_DEFERRAL_CONFIRM = """Perfect! I'll remind you next week, {name}. Feel free to ping me anytime when you're ready."""

ELIGIBILITY_DEVICE_DEFERRAL_FALLBACK = """Got it, {name}! I'll check back with you later when you're ready."""

ELIGIBILITY_DEVICE_REASK = """No problem. Could you confirm if you have a smartphone or laptop with internet?"""

ELIGIBILITY_DEVICE_OK = """Great! 👍"""

ELIGIBILITY_DEVICE_ACK = """Great! Thanks for confirming your device is ready, {name}!"""

ELIGIBILITY_COMMIT_PROMPT = """Children learn best when their teacher is consistent 😊 Can you give at least 2 hours a week so we can plan the kids' sessions well with you?
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

ELIGIBILITY_SUMMARY = """Lovely, thank you for sharing 💛

So far I've noted: you're {age_phrase}, have a working device & internet, and can give about {commitment_phrase} a week."""

ELIGIBILITY_AGE_DEVICE_FALLBACK = """Awesome! Just one more quick check — do you have a smartphone or laptop with a fairly stable internet connection?"""

ELIGIBILITY_DEVICE_COMMIT_FALLBACK = """Great! One last thing — can you spare about 2 hours a week so we can plan lessons with you?"""


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
PREFS_INTRO_COLLAB = """Let's find a rhythm that works for you and the students 🌱

Which 2–3 weekdays usually suit you? And do mornings, lunch hours, or early afternoons feel better?"""

PREFS_FOLLOWUP_DAYS = """Beautiful. Which weekdays would you lean on? You can name 2–3 that feel realistic."""

PREFS_FOLLOWUP_TIME = """Lovely! Do mornings, lunchtime, or early afternoon fit you best on those days?"""

PREFS_WEEKEND_NOTE = """Got it! Weekends are mostly shut in schools. Could we try picking any weekday slots that might work?"""

PREFS_EVENING_NUDGE = """Thanks for sharing. Most classroom slots wrap by 4 PM — is there any morning, lunch, or early afternoon window that could fit you?"""

PREFS_CONFIRM_DEFAULT = """Nice! I'll note {days} in {band} and plan around that."""

PREFS_SUMMARY_FALLBACK = """Brilliant — {days_label} in {band_label} should work well for the students."""


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
QA_ENTRY_PROMPT = """Do you have any quick questions for me? (training, certificate, subjects, tech setup…)

I'll keep it short and clear. 🙂"""

QA_MANDATORY_ORIENT = """One last step: a short WhatsApp check-in (about 10–15 minutes) is required for all volunteers.

It helps you understand the platform, classroom flow, and support available so you feel fully ready.

Please share 2–3 slots that work for you in the next few days (e.g., "Sat 4 PM" or "Sun 10 AM")."""

QA_CONTINUE_PROMPT = QA_MANDATORY_ORIENT

QA_NUDGE = QA_MANDATORY_ORIENT

QA_DEFERRAL_PROMPT = """No worries 😊 I can check back later. When should I remind you — Tue 10am, Thu 6pm, or Sat 10am?"""

QA_STOP_ACK = """Understood. I'll stop messages. If you change your mind, just say "Hi" here anytime. 💛"""

QA_SUMMARY_WITH_QUESTIONS = """Loved your questions — I'm glad we could cover them."""

QA_SUMMARY_NO_QUESTIONS = """Perfect, you're all set."""

QA_FAQ_ABOUT_SERVE = """SERVE helps thousands of children learn English, Science, and Maths through volunteers like you. You teach online — they learn in school — and our local coordinators make sure everything runs smoothly."""

QA_FAQ_TIME_PROCESS = """You'll teach live online while students sit in their school smart classroom.
Usually ~2 hours/week."""

QA_FAQ_SUPPORT = """Yes! Volunteer coordinator will share pedogogy and other information once you get assigned to a class, and a local coordinator supports you during classes."""

QA_FAQ_CERTIFICATE = """We provide a volunteer certificate after you complete the required sessions as per policy."""

QA_FAQ_SUBJECTS_GRADES = """Most volunteers teach English, Math or Science for grades 5–8 (varies by school).
We'll align your preferences during scheduling."""

QA_FAQ_TECH = """A tablet or laptop with stable internet is enough. We'll share the session link for classes."""

# ---------- Section 6: Orientation Scheduling ----------
ASK_AVAILABILITY = """One last step: a short WhatsApp check-in (about 10–15 minutes) is required for all volunteers.

It helps you understand the platform, classroom flow, and support available so you feel fully ready.

Please share 2–3 slots that work for you in the next few days (e.g., "Sat 4 PM" or "Sun 10 AM")."""

ORIENT_INTRO = """One last step: a short WhatsApp check-in so you feel fully ready.
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
ORIENT_AVAILABILITY_ACK = """Got it! I'll propose a couple of options that fit those times."""

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
Session: {slot}
Join link: {meet_link}
You'll get a reminder before the session. Welcome to the SERVE Volunteer Community 💛"""

ORIENT_INVALID_PICK = """I couldn't match that to the available slots 🙈 Could you please reply with one of the shown options (like 1 or 2), or type the exact day & time?"""

ORIENT_LATER_NOTE = """No worries 😊 You can message me here anytime to set this up. We'll keep your details ready."""


# ---------- Section 5: Final Confirmation ----------
DONE = """Session: {slot_label}
Join link: {meet_link}

Welcome to the SERVE Volunteer Community, {name}!
Every hour you share helps a child learn better. See you soon!"""


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


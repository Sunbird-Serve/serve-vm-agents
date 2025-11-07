"""
Hybrid Parsers for Teaching Preferences and Time Slots

This module implements rule-based extraction as the first pass,
with functions designed to be used by the SK hybrid plugin.
"""
import re
import logging
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)


# ---------- Subject Extraction ----------
SUBJECT_KEYWORDS = {
    "Math": ["math", "maths", "mathematics", "arithmetic", "algebra", "geometry"],
    "English": ["english", "eng"],
    "Science": ["science", "sci", "biology", "physics", "chemistry", "bio", "phy", "chem"],
    "Social Studies": ["social", "sst", "history", "geography", "civics", "geo", "hist"],
    "Computer Science": ["computer", "coding", "programming", "cs", "it", "computers", "comp sci"],
    "Hindi": ["hindi subject", "teach hindi"]  # Only match if explicitly teaching Hindi as subject
}

# Language indicators - check these first
LANGUAGE_INDICATORS = ["in hindi", "in english", "hindi medium", "english medium", "language"]


def extract_subjects_by_rules(text: str) -> list[str]:
    """
    Extract subjects using keyword matching
    
    Args:
        text: User's message
        
    Returns:
        List of matched subjects
        
    Examples:
        "Math and Science" → ["Math", "Science"]
        "I want to teach maths" → ["Math"]
        "computer programming" → ["Computer Science"]
        "Math in Hindi" → ["Math"] (not ["Math", "Hindi"])
    """
    subjects = []
    text_lower = text.lower()
    
    # Check if "hindi" or "english" is mentioned as language (not subject)
    is_language_context = any(indicator in text_lower for indicator in LANGUAGE_INDICATORS)
    
    for subject, keywords in SUBJECT_KEYWORDS.items():
        # Special handling for Hindi/English to avoid language confusion
        if subject in ["Hindi", "English"] and is_language_context:
            # Only match if explicitly "teach hindi" or "hindi subject"
            if any(keyword in text_lower for keyword in keywords):
                subjects.append(subject)
        else:
            # Normal matching for other subjects
            if any(keyword in text_lower for keyword in keywords):
                subjects.append(subject)
    
    log.debug(f"[PARSER] Rule-based subjects: {subjects} from '{text[:50]}...'")
    return subjects


# ---------- Grade Extraction ----------
GRADE_PATTERNS = [
    # Range patterns: "6-8", "6 to 8", "grade 6-8"
    (r'(\d+)\s*-\s*(\d+)', lambda m: f"{m.group(1)}-{m.group(2)}"),
    (r'(\d+)\s+to\s+(\d+)', lambda m: f"{m.group(1)}-{m.group(2)}"),
    (r'grade[s]?\s+(\d+)\s*-\s*(\d+)', lambda m: f"{m.group(1)}-{m.group(2)}"),
    (r'class(?:es)?\s+(\d+)\s*-\s*(\d+)', lambda m: f"{m.group(1)}-{m.group(2)}"),
    
    # Single grade: "grade 6", "class 8", "std 10"
    (r'grade[s]?\s+(\d+)', lambda m: m.group(1)),
    (r'class(?:es)?\s+(\d+)', lambda m: m.group(1)),
    (r'std\s+(\d+)', lambda m: m.group(1)),
    
    # Named ranges: "middle school", "high school", "elementary"
    (r'middle\s+school', lambda m: "6-8"),
    (r'high\s+school', lambda m: "9-12"),
    (r'elementary', lambda m: "1-5"),
    (r'primary', lambda m: "1-5"),
    (r'secondary', lambda m: "9-10"),
]


def extract_grades_by_rules(text: str) -> str:
    """
    Extract grade levels using regex patterns
    
    Args:
        text: User's message
        
    Returns:
        Extracted grade string (e.g., "6-8", "10") or empty string
        
    Examples:
        "grades 6-8" → "6-8"
        "class 10" → "10"
        "middle school" → "6-8"
    """
    text_lower = text.lower()
    
    for pattern, formatter in GRADE_PATTERNS:
        match = re.search(pattern, text_lower)
        if match:
            result = formatter(match)
            log.debug(f"[PARSER] Rule-based grades: {result} from '{text[:50]}...'")
            return result
    
    log.debug(f"[PARSER] No grades found by rules in '{text[:50]}...'")
    return ""


# ---------- Language Extraction ----------
LANGUAGE_PATTERNS = [
    # Explicit mentions
    (r'\bin\s+hindi\b', "Hindi"),
    (r'\bhindi\b(?!.*subject)', "Hindi"),  # Hindi but not as subject
    (r'\bin\s+english\b', "English"),
    (r'\benglish\b(?!.*subject)', "English"),
    (r'\bin\s+tamil\b', "Tamil"),
    (r'\btamil\b', "Tamil"),
    (r'\bin\s+kannada\b', "Kannada"),
    (r'\bkannada\b', "Kannada"),
    (r'\bin\s+telugu\b', "Telugu"),
    (r'\btelugu\b', "Telugu"),
    
    # Both languages
    (r'\bboth\b', "Both"),
    (r'hindi\s+and\s+english', "Both"),
    (r'english\s+and\s+hindi', "Both"),
    (r'bilingual', "Both"),
]


def extract_language_by_rules(text: str) -> str:
    """
    Extract teaching language using pattern matching
    
    Args:
        text: User's message
        
    Returns:
        Extracted language ("Hindi", "English", "Tamil", "Kannada", "Telugu", "Both") or empty string
        
    Examples:
        "in Hindi" → "Hindi"
        "both languages" → "Both"
        "tamil medium" → "Tamil"
        "english medium" → "English"
    """
    text_lower = text.lower()
    
    for pattern, language in LANGUAGE_PATTERNS:
        if re.search(pattern, text_lower):
            log.debug(f"[PARSER] Rule-based language: {language} from '{text[:50]}...'")
            return language
    
    log.debug(f"[PARSER] No language found by rules in '{text[:50]}...'")
    return ""


# ---------- Completeness Check ----------
def check_completeness(subjects: list[str], grades: str, language: str) -> dict:
    """
    Check if all required fields are present
    
    Args:
        subjects: List of subjects
        grades: Grade string
        language: Language string
        
    Returns:
        Dict with completeness info
    """
    has_subjects = bool(subjects)
    has_grades = bool(grades)
    has_language = bool(language)
    
    complete = has_subjects and has_grades and has_language
    missing = []
    
    if not has_subjects:
        missing.append("subjects")
    if not has_grades:
        missing.append("grades")
    if not has_language:
        missing.append("language")
    
    return {
        "complete": complete,
        "has_subjects": has_subjects,
        "has_grades": has_grades,
        "has_language": has_language,
        "missing": missing,
        "confidence": "high" if complete else "low"
    }


# ---------- Main Hybrid Parse Function ----------
def parse_teaching_preferences_rules_only(text: str) -> dict:
    """
    Extract teaching preferences using only rule-based methods
    
    This is the first pass before LLM fallback.
    
    Args:
        text: User's message with teaching preferences
        
    Returns:
        Dict with extracted data and metadata:
        {
            "subjects": [...],
            "grades": "...",
            "language": "...",
            "confidence": "high/medium/low",
            "method": "rules",
            "complete": True/False,
            "missing": [...]
        }
    """
    log.info(f"[PARSER] Rule-based parsing: '{text[:50]}...'")
    
    # Extract using rules
    subjects = extract_subjects_by_rules(text)
    grades = extract_grades_by_rules(text)
    language = extract_language_by_rules(text)
    
    # Check completeness
    completeness = check_completeness(subjects, grades, language)
    
    result = {
        "subjects": subjects,
        "grades": grades,
        "language": language,
        "method": "rules",
        "confidence": completeness["confidence"],
        "complete": completeness["complete"],
        "missing": completeness["missing"]
    }
    
    log.info(f"[PARSER] Rule-based result: subjects={len(subjects)}, grades={bool(grades)}, lang={bool(language)}, complete={result['complete']}")
    
    return result


# ========== TIME SLOT PARSING ==========

def iso_kolkata(dt: datetime) -> str:
    """Format datetime as ISO string with IST timezone"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")


def parse_simple_time(text: str) -> Optional[dict]:
    """
    Parse simple, common time patterns
    
    Handles:
    - "tomorrow 7pm" / "tomorrow at 7pm"
    - "Saturday 8pm" / "Saturday at 8:30pm"
    - "Monday 6pm" / "Mon 6:30pm"
    
    Returns:
        Dict with start_iso, end_iso, label or None if no match
    """
    text_lower = text.lower().strip()
    now = datetime.now()
    
    # Pattern: "tomorrow" + time
    tomorrow_pattern = r'tomorrow\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)'
    match = re.search(tomorrow_pattern, text_lower)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2)) if match.group(2) else 0
        ampm = match.group(3)
        
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        
        start = (now + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        end = start + timedelta(minutes=60)
        label = start.strftime("%a %d %b %I:%M %p")
        
        return {
            "start_iso": iso_kolkata(start),
            "end_iso": iso_kolkata(end),
            "label": label
        }
    
    # Pattern: Day of week + time (e.g., "Saturday 8pm", "Mon 6:30pm")
    weekdays = {
        'monday': 0, 'mon': 0,
        'tuesday': 1, 'tue': 1, 'tues': 1,
        'wednesday': 2, 'wed': 2,
        'thursday': 3, 'thu': 3, 'thur': 3, 'thurs': 3,
        'friday': 4, 'fri': 4,
        'saturday': 5, 'sat': 5,
        'sunday': 6, 'sun': 6
    }
    
    day_pattern = r'(monday|mon|tuesday|tue|tues|wednesday|wed|thursday|thu|thur|thurs|friday|fri|saturday|sat|sunday|sun)\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)'
    match = re.search(day_pattern, text_lower)
    if match:
        day_name = match.group(1)
        hour = int(match.group(2))
        minute = int(match.group(3)) if match.group(3) else 0
        ampm = match.group(4)
        
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        
        # Calculate next occurrence of this weekday
        target_weekday = weekdays[day_name]
        days_ahead = target_weekday - now.weekday()
        if days_ahead <= 0:  # Target day already passed this week
            days_ahead += 7
        
        start = (now + timedelta(days=days_ahead)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        end = start + timedelta(minutes=60)
        label = start.strftime("%a %d %b %I:%M %p")
        
        return {
            "start_iso": iso_kolkata(start),
            "end_iso": iso_kolkata(end),
            "label": label
        }
    
    return None


def extract_time_slots_by_rules(text: str) -> list[dict]:
    """
    Extract time slots using rule-based patterns
    
    Args:
        text: User's message with time preferences
        
    Returns:
        List of slot dicts with start_iso, end_iso, label
        Empty list if no patterns match
        
    Examples:
        "tomorrow 7pm" → [slot]
        "Saturday 8pm and Sunday 9pm" → [slot1, slot2]
        "next week sometime" → [] (too vague, needs MCP)
    """
    log.info(f"[PARSER] Trying rule-based time extraction for: '{text[:50]}...'")
    
    slots = []
    
    # Split by common separators to find multiple times
    segments = re.split(r'\s+and\s+|\s+or\s+|,\s*', text.lower())
    
    for segment in segments:
        slot = parse_simple_time(segment)
        if slot:
            slots.append(slot)
            log.debug(f"[PARSER] Found time slot by rules: {slot['label']}")
    
    if slots:
        log.info(f"[PARSER] Rule-based time parsing SUCCESS: {len(slots)} slots found")
    else:
        log.info(f"[PARSER] Rule-based time parsing FAILED: No patterns matched")
    
    return slots


def check_time_completeness(slots: list) -> dict:
    """
    Check if time slot extraction is complete
    
    Args:
        slots: List of extracted slots
        
    Returns:
        Dict with completeness info
    """
    has_slots = bool(slots) and len(slots) >= 1
    
    return {
        "complete": has_slots,
        "num_slots": len(slots),
        "confidence": "high" if has_slots else "low"
    }


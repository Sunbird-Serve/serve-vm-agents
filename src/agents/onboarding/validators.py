"""
Validators and Normalizers for Onboarding Data

Validates extracted data against business rules and normalizes formats.
"""
import logging
from typing import Tuple

log = logging.getLogger(__name__)


# ---------- Catalogs ----------
VALID_SUBJECTS = [
    "Math",
    "English", 
    "Science",
    "Social Studies",
    "Computer Science",
    "Hindi"
]

VALID_GRADE_RANGES = [
    "1-3",
    "4-5",
    "6-8",
    "9-10",
    "11-12"
]

VALID_LANGUAGES = [
    "Hindi", 
    "English", 
    "Tamil", 
    "Kannada", 
    "Telugu",
    "Marathi",
    "Bengali",
    "Gujarati",
    "Both"
]


# ---------- Subject Validation ----------
def validate_subjects(subjects: list[str]) -> Tuple[bool, list[str]]:
    """
    Validate and normalize subject list
    
    Args:
        subjects: List of subject names
        
    Returns:
        (is_valid, normalized_subjects)
        
    Examples:
        ["Math", "Science"] → (True, ["Math", "Science"])
        ["Math", "Invalid"] → (False, ["Math"])
        [] → (False, [])
    """
    if not subjects:
        log.warning("[VALIDATOR] No subjects provided")
        return False, []
    
    # Normalize and validate
    normalized = []
    for subject in subjects:
        # Find case-insensitive match
        matched = next(
            (valid for valid in VALID_SUBJECTS if valid.lower() == subject.lower()),
            None
        )
        if matched:
            normalized.append(matched)
        else:
            log.warning(f"[VALIDATOR] Invalid subject: {subject}")
    
    is_valid = len(normalized) > 0
    
    log.info(f"[VALIDATOR] Subjects: {subjects} → Valid={is_valid}, Normalized={normalized}")
    return is_valid, normalized


# ---------- Grade Validation ----------
def normalize_grade_range(grades: str) -> str:
    """
    Normalize grade range to standard format
    
    Args:
        grades: Grade string (e.g., "6-8", "10", "middle school")
        
    Returns:
        Normalized grade string
        
    Examples:
        "6-8" → "6-8"
        "10" → "10"
        "9" → "9"
    """
    if not grades:
        return ""
    
    # Already in standard format
    if grades in VALID_GRADE_RANGES:
        return grades
    
    # Single digit
    if grades.isdigit():
        return grades
    
    # Range format (should already be normalized by parser)
    if "-" in grades:
        parts = grades.split("-")
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            return f"{parts[0].strip()}-{parts[1].strip()}"
    
    return grades


def validate_grades(grades: str) -> Tuple[bool, str]:
    """
    Validate grade levels
    
    Args:
        grades: Grade string
        
    Returns:
        (is_valid, normalized_grades)
        
    Examples:
        "6-8" → (True, "6-8")
        "10" → (True, "10")
        "" → (False, "")
    """
    if not grades:
        log.warning("[VALIDATOR] No grades provided")
        return False, ""
    
    normalized = normalize_grade_range(grades)
    
    # Valid if it's in catalog or is a single digit 1-12
    is_valid = (
        normalized in VALID_GRADE_RANGES or
        (normalized.isdigit() and 1 <= int(normalized) <= 12)
    )
    
    log.info(f"[VALIDATOR] Grades: {grades} → Valid={is_valid}, Normalized={normalized}")
    return is_valid, normalized


# ---------- Language Validation ----------
def validate_language(language: str) -> Tuple[bool, str]:
    """
    Validate teaching language
    
    Args:
        language: Language string
        
    Returns:
        (is_valid, normalized_language)
        
    Examples:
        "Hindi" → (True, "Hindi")
        "both" → (True, "Both")
        "spanish" → (False, "")
    """
    if not language:
        log.warning("[VALIDATOR] No language provided, defaulting to English")
        return True, "English"  # Default
    
    # Normalize case
    normalized = language.strip().title()
    
    # Check against valid languages
    matched = next(
        (valid for valid in VALID_LANGUAGES if valid.lower() == normalized.lower()),
        None
    )
    
    if matched:
        log.info(f"[VALIDATOR] Language: {language} → Valid=True, Normalized={matched}")
        return True, matched
    
    log.warning(f"[VALIDATOR] Invalid language: {language}")
    return False, ""


# ---------- Complete Profile Validation ----------
def validate_teaching_profile(subjects: list[str], grades: str, language: str) -> dict:
    """
    Validate complete teaching profile
    
    Args:
        subjects: List of subjects
        grades: Grade string
        language: Language string
        
    Returns:
        Dict with validation results:
        {
            "valid": True/False,
            "subjects": [...],
            "grades": "...",
            "language": "...",
            "errors": [...]
        }
    """
    errors = []
    
    # Validate subjects
    subjects_valid, validated_subjects = validate_subjects(subjects)
    if not subjects_valid:
        errors.append("Invalid or missing subjects")
    
    # Validate grades
    grades_valid, validated_grades = validate_grades(grades)
    if not grades_valid:
        errors.append("Invalid or missing grades")
    
    # Validate language
    language_valid, validated_language = validate_language(language)
    if not language_valid:
        errors.append("Invalid language")
    
    is_valid = subjects_valid and grades_valid and language_valid
    
    result = {
        "valid": is_valid,
        "subjects": validated_subjects,
        "grades": validated_grades,
        "language": validated_language,
        "errors": errors
    }
    
    log.info(f"[VALIDATOR] Profile validation: Valid={is_valid}, Errors={errors}")
    return result


# ---------- Helper Functions ----------
def is_yes_response(text: str) -> bool:
    """Check if text is a yes/affirmative response"""
    text_lower = text.lower().strip()
    
    # Check for phrase patterns (more flexible)
    yes_prefixes = ["yes", "yeah", "ya", "yup", "yep", "correct", "right", "sure", "ok", "okay", "sounds good", "sounds great", "absolutely", "definitely"]
    for prefix in yes_prefixes:
        if text_lower.startswith(prefix):
            return True
    
    # Check for exact matches
    yes_words = {
        "yes", "y", "ok", "okay", "sure", "go ahead", "continue", "proceed", 
        "ya", "haan", "start", "begin", "let's start", "lets start", 
        "correct", "right", "yup", "yep", "that's right", "thats right",
        "perfect", "exactly", "absolutely", "definitely", "of course",
        "sounds good", "sounds great", "good", "great", "fine", "alright",
        "sure thing", "why not", "i agree", "agreed", "true", "indeed",
        "perfectly ok", "perfectly okay", "sounds fine", "works", "works for me",
        "that sounds good", "that works", "that's fine", "thats fine",
        "no problem", "not an issue", "im good", "i'm good", "im ok", "i'm ok",
        "let's go", "lets go", "i'm in", "im in", "count me in", "sign me up",
        "i can do that", "i'm interested", "im interested", "definitely yes",
        "sounds perfect", "sounds excellent", "i'm ready", "im ready", "ready",
        "bring it on", "i'm game", "im game", "yes please", "please proceed"
    }
    return text_lower in yes_words


def is_no_response(text: str) -> bool:
    """Check if text is a no/negative response"""
    no_words = {"no", "n", "nope", "nah", "not really", "no thanks", "not interested",
                "don't want", "dont want", "not now", "pass", "skip", "decline"}
    return text.lower().strip() in no_words


def normalize_phone(phone: str) -> str:
    """Normalize phone number (remove + and whitespace)"""
    return (phone or "").lstrip("+").strip()


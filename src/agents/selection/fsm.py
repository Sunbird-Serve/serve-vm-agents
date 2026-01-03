"""
Selection Agent - State Machine Definitions
"""
from .types import SelectionState, RecommendationOutcome

# State definitions
SELECTION_STATES = {
    SelectionState.START: "Entry; send credibility message + video link",
    SelectionState.Q1_INTENT: "Ask 'What made you interested in volunteering with SERVE?'",
    SelectionState.Q2_LANGUAGE: "Ask 'Which language(s) are you comfortable teaching in?'",
    SelectionState.EVALUATE: "Internal; compute decision (for now always recommended=true)",
    SelectionState.RECOMMENDED: "Send 'You're all set, Let's look at open opportunities.'",
    SelectionState.NOT_RECOMMENDED: "Placeholder message + community link (not used now)",
}

# Recommendation outcomes
RECOMMENDATION_OUTCOMES = {
    RecommendationOutcome.RECOMMENDED: "Volunteer is recommended to proceed",
    RecommendationOutcome.NOT_RECOMMENDED: "Volunteer is not recommended",
    RecommendationOutcome.PENDING: "Decision pending, needs more information",
}

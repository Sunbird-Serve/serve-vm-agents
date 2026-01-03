"""
Fulfillment Agent - State Machine Definitions
"""
from .types import FulfillmentState

# State definitions
FULFILLMENT_STATES = {
    FulfillmentState.INTRO: "Ask consent to see open opportunities (Yes/No)",
    FulfillmentState.LIST: "Fetch needs (stub), render list, prompt to pick 1..N",
    FulfillmentState.WAIT_PICK: "Parse reply, validate selection",
    FulfillmentState.NOMINATE: "Call nominate (stub)",
    FulfillmentState.DONE: "Confirmation",
    FulfillmentState.EXIT: "If user says No",
}

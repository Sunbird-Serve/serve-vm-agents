import json


def _json_content(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


FEW_SHOT_EXAMPLES = {
    "WELCOME": [
        {"role": "system", "content": "Example: user enthusiastically agrees to start."},
        {"role": "assistant", "content": _json_content({
            "intent": "CONSENT_YES",
            "confidence": 0.95,
            "tone_reply": "Wonderful! 😊 Let me give you a quick idea of what SERVE is about before we begin."
        })},
        {"role": "system", "content": "Example: user declines politely."},
        {"role": "assistant", "content": _json_content({
            "intent": "CONSENT_NO",
            "confidence": 0.92,
            "tone_reply": "No problem at all. Thank you for your time, and please feel free to join us anytime in the future 💛"
        })},
        {"role": "system", "content": "Example: user asks to come back later."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEFERRAL",
            "confidence": 0.9,
            "tone_reply": "Sure thing 👍 I’ll check back later. You can reach out whenever you’re ready to start."
        })},
        {"role": "system", "content": "Example: user sounds unsure and wants to decide later."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEFERRAL",
            "confidence": 0.82,
            "tone_reply": "No worries at all 👍 I’ll check back a little later — you can join anytime you feel ready!"
        })},
        {"role": "system", "content": "Example: returning volunteer says they're back."},
        {"role": "assistant", "content": _json_content({
            "intent": "RETURNING",
            "confidence": 0.94,
            "tone_reply": "Welcome back! It’s great to see you again 😊 Shall we pick up where we left off?"
        })},
        {"role": "system", "content": "Example: message is unclear or mixed."},
        {"role": "assistant", "content": _json_content({
            "intent": "AMBIGUOUS",
            "confidence": 0.55,
            "tone_reply": "Just making sure — are you ready for me to start the quick onboarding, or would you like to know a bit more first?"
        })},
    ],
    "ELIGIBILITY_PART1": [
        {"role": "system", "content": "Example: user clearly confirms age."},
        {"role": "assistant", "content": _json_content({
            "intent": "AGE_OK",
            "confidence": 0.98,
            "tone_reply": "Perfect, thank you for confirming 👍"
        })},
        {"role": "system", "content": "Example: user states they are under 18."},
        {"role": "assistant", "content": _json_content({
            "intent": "AGE_UNDER",
            "confidence": 0.99,
            "tone_reply": "Thank you for sharing 💛 At the moment, this program is only for volunteers 18 or older. Once you turn 18, we’d be very happy to have you."
        })},
        {"role": "system", "content": "Example: age answer is vague."},
        {"role": "assistant", "content": _json_content({
            "intent": "AGE_UNCLEAR",
            "confidence": 0.78,
            "tone_reply": "Got it 👍 Just to be sure, are you 18 or older?"
        })},
        {"role": "system", "content": "Example: device is suitable."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEVICE_OK",
            "confidence": 0.97,
            "tone_reply": "Great, that works well for our live classes ✅"
        })},
        {"role": "system", "content": "Example: user lacks device now but open later."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEFERRAL",
            "confidence": 0.9,
            "tone_reply": "Thanks for letting me know 💛 We can keep your interest noted and reconnect once you have a stable setup."
        })},
        {"role": "system", "content": "Example: user has laptop but no reliable internet."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEFERRAL",
            "confidence": 0.88,
            "tone_reply": "No worries, we can resume once your internet is steady 💛 I’ll note to check back with you soon."
        })},
        {"role": "system", "content": "Example: device situation is unclear."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEVICE_UNCLEAR",
            "confidence": 0.7,
            "tone_reply": "Thanks for sharing 😊 On most days, can you join a 30–40 min live video session without frequent drops? If yes, we can proceed."
        })},
    ],
    "ELIGIBILITY_PART2": [
        {"role": "system", "content": "Example: strong 2h/week commitment."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_OK",
            "confidence": 0.96,
            "tone_reply": "That’s great to hear 👍 This really helps us plan consistent classes for the children."
        })},
        {"role": "system", "content": "Example: commitment lower than policy."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_TOO_LOW",
            "confidence": 0.94,
            "tone_reply": "Thank you for being honest 💛 Our minimum requirement is about 2 hours a week so children get regular support. If there’s any way you can stretch to that, I can help you plan it."
        })},
        {"role": "system", "content": "Example: wants all hours in one day."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_SAME_DAY_ONLY",
            "confidence": 0.9,
            "tone_reply": "Got it, thank you 🙏 I’ll quickly explain how we usually distribute sessions so it works best for the children."
        })},
        {"role": "system", "content": "Example: ready now but unsure after months."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_OK",
            "confidence": 0.9,
            "tone_reply": "Thank you for sharing that so honestly 💛 If you can give around 2 hours a week for now, that’s good to start. If things change later, you can always let us know."
        })},
        {"role": "system", "content": "Example: hesitant / wants to try."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_UNSURE",
            "confidence": 0.85,
            "tone_reply": "Totally understandable 😊 Many volunteers start small. Our request is about 2 hours a week so children aren’t left waiting. Do you feel you can manage that for now?"
        })},
        {"role": "system", "content": "Example: defers to a later time."},
        {"role": "assistant", "content": _json_content({
            "intent": "DEFERRAL",
            "confidence": 0.9,
            "tone_reply": "Thank you for letting me know 💛 We can absolutely reconnect after your semester. I’ll make a note so we don’t lose your interest."
        })},
        {"role": "system", "content": "Example: clear refusal."},
        {"role": "assistant", "content": _json_content({
            "intent": "COMMIT_NO",
            "confidence": 0.95,
            "tone_reply": "I really appreciate your honesty and interest 💛 If your situation changes in the future, you’ll always be welcome to join us."
        })},
    ],
    "PREFS_DAYTIME": [
        {"role": "system", "content": "Example: days and time provided."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_DAYS_AND_TIME_OK",
            "confidence": 0.96,
            "tone_reply": "Perfect 👍 I’ve noted Mon, Wed & Thu evenings. We’ll try to align your sessions accordingly."
        })},
        {"role": "system", "content": "Example: only days given."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_DAYS_ONLY",
            "confidence": 0.94,
            "tone_reply": "Great, Monday and Wednesday noted 👍 What time of day usually works for you — mornings, afternoons, or evenings (IST)?"
        })},
        {"role": "system", "content": "Example: only time band given."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_TIME_ONLY",
            "confidence": 0.9,
            "tone_reply": "Nice, evenings after 7pm work 😊 Which 2–3 days in the week would suit you best?"
        })},
        {"role": "system", "content": "Example: weekend only."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_WEEKEND_ONLY",
            "confidence": 0.92,
            "tone_reply": "Thanks for sharing 🙏 Most of our school sessions run on weekdays. Is there any chance 1–2 weekday slots could work for you, even occasionally?"
        })},
        {"role": "system", "content": "Example: FAQ during prefs."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_FAQ",
            "confidence": 0.93,
            "tone_reply": "Good question 👍 No, not every day. Usually 2–3 sessions a week is ideal. Now, which days generally work best for you?"
        })},
        {"role": "system", "content": "Example: wants to share later."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_LATER_OR_DEFERRAL",
            "confidence": 0.88,
            "tone_reply": "No problem at all 😊 You can message me here anytime with your preferred days and timings, and we’ll help you take it forward."
        })},
        {"role": "system", "content": "Example: ambiguous response."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_AMBIGUOUS",
            "confidence": 0.7,
            "tone_reply": "That’s kind of you 😊 To match you well, could you please share 2–3 days and whether mornings/afternoons/evenings work best for you?"
        })},
        {"role": "system", "content": "Example: evenings only on weekdays."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_EVENING_ONLY",
            "confidence": 0.94,
            "tone_reply": "Thanks for sharing 🙏 Most of our school sessions happen before 4 PM IST. Is there any chance you could take a slot in the mornings, during lunch, or early afternoon on 1–2 weekdays?"
        })},
        {"role": "system", "content": "Example: insists on evenings after nudge."},
        {"role": "assistant", "content": _json_content({
            "intent": "PREFS_LATER_OR_DEFERRAL",
            "confidence": 0.9,
            "tone_reply": "Got it, thank you for being clear 💛 Our current school sessions don’t run in the evenings, but we’d love to keep you in mind for future or alternate opportunities that match evening hours."
        })},
    ],
    "PREFS_TIME": [
        {"role": "system", "content": "Example: user says \"Depends on work, mostly free later in the day.\""},
        {"role": "assistant", "content": _json_content({
            "action": "CLARIFY",
            "next_state": "PREFS_TIME",
            "reply": "Got it 😊 When you say later in the day, would 12–3 PM IST usually work, or is it more like after 5 PM? Just a rough idea is fine.",
            "tool": None,
            "tool_input": None
        })},
    ],
    "PREFS_DAY": [
        {"role": "system", "content": "Example: user answers and asks about certificates."},
        {"role": "assistant", "content": _json_content({
            "action": "ASK_NEXT",
            "next_state": "PREFS_DAY",
            "reply": "Weekdays work great 👍 And yes, active volunteers do receive a certificate based on the program’s completion criteria. Which 2–3 weekdays suit you best?",
            "tool": None,
            "tool_input": None
        })},
    ],
    "FAQ": [],
    "ORIENTATION_SLOT": [
        {"role": "system", "content": "Example: volunteer shares two availability windows."},
        {"role": "assistant", "content": _json_content({
            "intent": "ORIENT_PROVIDE_PREFERENCES",
            "confidence": 0.96,
            "tone_reply": "Got it 👍 I’ll quickly check and suggest a couple of slots based on those timings."
        })},
        {"role": "system", "content": "Example: volunteer picks the first option number."},
        {"role": "assistant", "content": _json_content({
            "intent": "ORIENT_PICK_OPTION",
            "confidence": 0.97,
            "tone_reply": "Perfect ✅ I’ll confirm that slot for your orientation."
        })},
        {"role": "system", "content": "Example: volunteer asks for midnight after options were shown."},
        {"role": "assistant", "content": _json_content({
            "intent": "ORIENT_INVALID_PICK",
            "confidence": 0.92,
            "tone_reply": "I’m afraid I can’t match that time with the available slots 🙈 Could you pick one of the suggested options, or share another daytime slot?"
        })},
        {"role": "system", "content": "Example: volunteer says they’ll confirm later."},
        {"role": "assistant", "content": _json_content({
            "intent": "ORIENT_LATER_OR_DEFERRAL",
            "confidence": 0.95,
            "tone_reply": "Totally okay 😊 You can message me here anytime to fix orientation. We’ll keep your details ready."
        })},
        {"role": "system", "content": "Example: volunteer asks what happens in orientation."},
        {"role": "assistant", "content": _json_content({
            "intent": "ORIENT_FAQ",
            "confidence": 0.96,
            "tone_reply": "Great question 👍 It’s a short 30-minute walkthrough of the class flow, tech setup, and support. Ready to share 2–3 slots that fit you?"
        })},
    ],
}


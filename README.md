# serve-vm-agents
The workers that execute agent policies (Onboarding, Screening, Fulfillment). Implements step-level logic (e.g., slot filling, meet creation, eligibility checks).  Calls external services or MCP tools to perform tasks.  Runs as multiple deployments (OnboardingWorker, ScreeningWorker, FulfillmentWorker) but lives in a single codebase for simplicity.

from dataclasses import dataclass

@dataclass
class IntakeResult:
    caller_name: str
    company: str
    reason: str
    urgency: str
    recommended_action: str

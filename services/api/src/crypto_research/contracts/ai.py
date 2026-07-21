from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field

from crypto_research.contracts.base import UTCModel


class AIOpinion(StrEnum):
    SUPPORT = "SUPPORT"
    OPPOSE = "OPPOSE"
    UNCERTAIN = "UNCERTAIN"


class PrincipleCitation(UTCModel):
    skill: str
    section: str


class AIAssessment(UTCModel):
    schema_version: str = "1.0.0"
    assessment_id: UUID
    snapshot_id: UUID
    opinion: AIOpinion
    reasons: tuple[str, ...] = Field(min_length=1)
    citations: tuple[PrincipleCitation, ...] = Field(min_length=1)
    risk_notes: tuple[str, ...]
    market_data_cutoff: datetime
    model_id: str
    prompt_version: str
    skill_version: str

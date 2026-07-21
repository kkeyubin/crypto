// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type AssessmentId = string;
/**
 * @minItems 1
 */
export type Citations = [PrincipleCitation, ...PrincipleCitation[]];
export type Section = string;
export type Skill = string;
export type MarketDataCutoff = string;
export type ModelId = string;
export type AIOpinion = "SUPPORT" | "OPPOSE" | "UNCERTAIN";
export type PromptVersion = string;
/**
 * @minItems 1
 */
export type Reasons = [string, ...string[]];
export type RiskNotes = string[];
export type SchemaVersion = string;
export type SkillVersion = string;
export type SnapshotId = string;

interface AIAssessmentShape {
  assessment_id: AssessmentId;
  citations: Citations;
  market_data_cutoff: MarketDataCutoff;
  model_id: ModelId;
  opinion: AIOpinion;
  prompt_version: PromptVersion;
  reasons: Reasons;
  risk_notes: RiskNotes;
  schema_version?: SchemaVersion;
  skill_version: SkillVersion;
  snapshot_id: SnapshotId;
}
export interface PrincipleCitation {
  section: Section;
  skill: Skill;
}

export type AIAssessment = DeepReadonly<AIAssessmentShape>;

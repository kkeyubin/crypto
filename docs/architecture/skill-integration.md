# Skill Integration

## Canonical Source

The repository owns the unified trading-research Skill. Deployment copies the validated version to the Codex user Skills directory and the server's Hermes Skills environment. The repository version and content hash remain canonical.

## Responsibility Boundary

The Skill may:

- translate Nison/Volman ideas into falsifiable hypotheses;
- generate or audit a `StrategySpec`;
- identify ambiguity and required labels;
- design Aronson-compatible experiments;
- interpret reports and produce AI shadow assessments.

The Skill may not:

- activate a draft strategy;
- mutate a frozen version;
- access broker credentials;
- place, cancel, resize, or veto paper orders;
- hide failed parameter searches;
- declare profitability from in-sample output.

## Book Weighting

Volman is the primary price-action hypothesis source. Nison contributes optional background and confirmation variables. Aronson defines the mandatory research and evidence controls.

MVP order-producing strategy families are BB and RB. DD, FB, SB, IRB, and ARB are observation-only AI/human labels. Nison conditions are not automatically combined with each Volman setup; each added filter is a separately declared hypothesis.

## Runtime AI Contract

The AI worker serializes only past and current information into a frozen `MarketSnapshot`. Hermes loads the unified Skill and returns schema-validated `SUPPORT`, `OPPOSE`, or `UNCERTAIN` assessment data with reasons, cited principles, risk notes, data cutoff, and model/prompt/Skill versions.

The assessment is stored after the deterministic signal and does not change its order path. Later research compares the assessment against outcomes under an Aronson-compatible preregistered procedure.

## Reproducibility

Every generated specification or assessment records:

- Skill name, version, and content hash;
- model/provider identifier;
- prompt-template version;
- data manifest and cutoff;
- schema version;
- validation errors and retries;
- human approval identity and timestamp when applicable.

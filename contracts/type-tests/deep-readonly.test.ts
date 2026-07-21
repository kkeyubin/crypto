import type { StrategySpec } from "../types/index";

declare const strategy: StrategySpec;

// @ts-expect-error generated root contracts are deeply readonly
strategy.identity.name = "changed";

// @ts-expect-error tuple elements nested under a root contract are readonly
strategy.volman.chronology[0] = "changed";

// @ts-expect-error index-signature values nested under a root contract are readonly
strategy.parameters.fixed.side = "short";

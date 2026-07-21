import assert from "node:assert/strict";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const schemaSource = path.join(root, "contracts", "jsonschema");
const generatedHeader = "// Generated. Do not edit.\n";
const expectedRoots = [
  "AIAssessment",
  "AddSymbolRequest",
  "BackfillRequest",
  "DataGapView",
  "DataManifest",
  "DataPartitionView",
  "EligibilityView",
  "IngestionJobView",
  "MarketDataHealthView",
  "MarketSnapshot",
  "StrategySpec",
  "StrategySpecRecord",
  "StreamStateView",
  "SymbolProfileView",
  "SymbolView",
];
const temporaryRoot = await mkdtemp(path.join(tmpdir(), "crypto-contract-types-"));
const inputDir = path.join(temporaryRoot, "jsonschema");
const outputDir = path.join(temporaryRoot, "types");

try {
  await cp(schemaSource, inputDir, { recursive: true });
  await mkdir(outputDir, { recursive: true });
  await writeFile(path.join(outputDir, "manual.ts"), "export const manual = true;\n");
  await writeFile(path.join(outputDir, "Stale.ts"), `${generatedHeader}export type Stale = string;\n`);

  const arguments_ = [
    path.join(root, "contracts", "generate-types.mjs"),
    "--input",
    inputDir,
    "--output",
    outputDir,
  ];
  await execFileAsync(process.execPath, arguments_);
  assert.equal(await readFile(path.join(outputDir, "manual.ts"), "utf8"), "export const manual = true;\n");
  await assert.rejects(readFile(path.join(outputDir, "Stale.ts"), "utf8"));

  const firstRun = await Promise.all(
    (await readdir(outputDir))
      .filter((name) => name !== "manual.ts")
      .sort()
      .map(async (name) => [name, await readFile(path.join(outputDir, name), "utf8")]),
  );
  await execFileAsync(process.execPath, arguments_);
  const secondRun = await Promise.all(
    (await readdir(outputDir))
      .filter((name) => name !== "manual.ts")
      .sort()
      .map(async (name) => [name, await readFile(path.join(outputDir, name), "utf8")]),
  );
  assert.deepEqual(secondRun, firstRun);
  assert.deepEqual(
    (await readFile(path.join(outputDir, "index.ts"), "utf8"))
      .trim()
      .split("\n")
      .slice(2),
    expectedRoots.map((rootName) => `export type { ${rootName} } from "./${rootName}";`),
  );
  for (const rootName of expectedRoots) {
    const declaration = await readFile(path.join(outputDir, `${rootName}.ts`), "utf8");
    assert.match(declaration, /type DeepReadonly<T> =/);
    assert.match(declaration, new RegExp(`interface ${rootName}Shape \\{`));
    assert.match(
      declaration,
      new RegExp(`export type ${rootName} = DeepReadonly<${rootName}Shape>;`),
    );
    assert.doesNotMatch(declaration, new RegExp(`export interface ${rootName} \\{`));
  }
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}

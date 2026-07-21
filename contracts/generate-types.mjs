import { compileFromFile } from "json-schema-to-typescript";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";

const GENERATED_HEADER = "// Generated. Do not edit.";
const EXPECTED_ROOTS = [
  "AIAssessment",
  "DataManifest",
  "MarketSnapshot",
  "StrategySpec",
  "StrategySpecRecord",
];

const DEEP_READONLY = `type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;`;

function makeRootDeepReadonly(root, declaration) {
  const exportedRoot = `export interface ${root} {`;
  if (!declaration.includes(exportedRoot)) {
    throw new Error(`generated declaration is missing root interface: ${root}`);
  }
  const internalShape = declaration.replace(exportedRoot, `interface ${root}Shape {`);
  return `${DEEP_READONLY}\n\n${internalShape.trimEnd()}\n\nexport type ${root} = DeepReadonly<${root}Shape>;\n`;
}

function parseArguments(arguments_) {
  const values = {
    inputDir: path.resolve("contracts/jsonschema"),
    outputDir: path.resolve("contracts/types"),
  };
  for (let index = 0; index < arguments_.length; index += 2) {
    const option = arguments_[index];
    const value = arguments_[index + 1];
    if ((option !== "--input" && option !== "--output") || !value) {
      throw new Error("usage: generate-types.mjs [--input DIR] [--output DIR]");
    }
    values[option === "--input" ? "inputDir" : "outputDir"] = path.resolve(value);
  }
  return values;
}

const { inputDir, outputDir } = parseArguments(process.argv.slice(2));
const files = (await readdir(inputDir))
  .filter((name) => name.endsWith(".schema.json"))
  .sort();
const roots = files.map((file) => file.replace(".schema.json", ""));
const expectedRoots = [...EXPECTED_ROOTS].sort();
if (roots.join("\n") !== expectedRoots.join("\n")) {
  const missing = expectedRoots.filter((root) => !roots.includes(root));
  const stale = roots.filter((root) => !expectedRoots.includes(root));
  throw new Error(
    `schema set mismatch: missing=[${missing.join(", ")}], stale=[${stale.join(", ")}]`,
  );
}
await mkdir(outputDir, { recursive: true });
for (const existing of await readdir(outputDir, { withFileTypes: true })) {
  if (!existing.isFile() || !existing.name.endsWith(".ts")) continue;
  const target = path.join(outputDir, existing.name);
  const firstLine = (await readFile(target, "utf8")).split("\n", 1)[0];
  if (firstLine === GENERATED_HEADER) await rm(target);
}
for (const file of files) {
  const root = file.replace(".schema.json", "");
  const declaration = await compileFromFile(path.join(inputDir, file), {
    bannerComment: "",
  });
  await writeFile(
    path.join(outputDir, `${root}.ts`),
    `${GENERATED_HEADER}\n\n${makeRootDeepReadonly(root, declaration)}`,
  );
}
const index = roots
  .map((root) => `export type { ${root} } from "./${root}";`)
  .join("\n");
await writeFile(
  path.join(outputDir, "index.ts"),
  "// Generated. Do not edit.\n\n" + index + "\n",
);

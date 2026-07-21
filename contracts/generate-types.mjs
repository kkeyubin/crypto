import { compileFromFile } from "json-schema-to-typescript";
import { mkdir, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";

const inputDir = path.resolve("contracts/jsonschema");
const outputDir = path.resolve("contracts/types");
const files = (await readdir(inputDir))
  .filter((name) => name.endsWith(".schema.json"))
  .sort();
const roots = [];
await mkdir(outputDir, { recursive: true });
for (const existing of await readdir(outputDir)) {
  if (existing.endsWith(".ts")) await rm(path.join(outputDir, existing));
}
for (const file of files) {
  const root = file.replace(".schema.json", "");
  const declaration = await compileFromFile(path.join(inputDir, file), {
    bannerComment: "",
  });
  await writeFile(
    path.join(outputDir, `${root}.ts`),
    "// Generated. Do not edit.\n\n" + declaration,
  );
  roots.push(root);
}
const index = roots
  .map((root) => `export type { ${root} } from "./${root}";`)
  .join("\n");
await writeFile(
  path.join(outputDir, "index.ts"),
  "// Generated. Do not edit.\n\n" + index + "\n",
);

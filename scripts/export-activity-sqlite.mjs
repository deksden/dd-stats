#!/usr/bin/env node

import { promises as fs } from "node:fs";
import path from "node:path";
import { writeSqliteSnapshot } from "./fetch-github-activity.mjs";

async function main() {
  const projectRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
  const dataDir = path.join(projectRoot, "data");
  const sqlitePath = path.join(dataDir, "github-activity.sqlite");
  const dataFiles = (await fs.readdir(dataDir))
    .filter((file) => /^\d{4}\.json$/.test(file))
    .sort();

  if (!dataFiles.length) {
    throw new Error("No yearly JSON files were found in ./data");
  }

  const yearSnapshots = await Promise.all(
    dataFiles.map(async (file) => JSON.parse(await fs.readFile(path.join(dataDir, file), "utf8"))),
  );
  const [sample] = yearSnapshots;
  const organizations = (sample.scope?.organizations ?? []).map((login) => ({ login }));

  await writeSqliteSnapshot({
    sqlitePath,
    viewer: sample.viewer,
    organizations,
    yearSnapshots,
  });

  console.log(`SQLite snapshot written to ${sqlitePath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

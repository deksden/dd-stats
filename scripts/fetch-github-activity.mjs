#!/usr/bin/env node

import { execFile as execFileCallback } from "node:child_process";
import { promisify } from "node:util";
import { promises as fs } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const execFile = promisify(execFileCallback);
const MAX_BUFFER = 32 * 1024 * 1024;
const COMMIT_MARKER = "--COMMIT--";
const WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

async function main() {
  const projectRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
  const args = parseArgs(process.argv.slice(2));
  const configPath = args.config ?? path.join(projectRoot, "config", "activity.config.json");
  const config = JSON.parse(await fs.readFile(configPath, "utf8"));
  const years = args.years.length ? args.years : config.years;
  const timezone = args.timezone ?? config.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone;
  const fetchRemotes = args.fetchRemotes ?? config.fetchRemotes ?? true;
  const cloneMissingRepos = args.cloneMissingRepos ?? config.cloneMissingRepos ?? true;
  const localRepoRoots = normalizePaths(config.localRepoRoots ?? []);
  const outputDir = path.join(projectRoot, "data");
  const cacheDir = path.join(projectRoot, ".cache", "repos");
  const sqlitePath = path.join(outputDir, "github-activity.sqlite");

  await fs.mkdir(outputDir, { recursive: true });
  await fs.mkdir(cacheDir, { recursive: true });

  console.log(`Using timezone: ${timezone}`);
  console.log(`Scanning local repositories under ${localRepoRoots.length} roots...`);

  const localRepoIndex = await discoverLocalRepos(localRepoRoots);
  const viewer = await getViewer();
  const organizations = await getOrganizations();
  const repoCatalog = await getRepositoryCatalog({
    viewer,
    organizations,
    config,
    localRepoIndex,
  });
  const identities = buildAuthorIdentities(viewer);
  const metadataCache = new Map();
  const repoPathCache = new Map();
  const defaultBranchProbeCache = new Map();
  const yearSnapshots = [];

  for (const year of years) {
    console.log(`\nCollecting GitHub activity for ${year}...`);
    const boundaries = buildYearBoundaries(year);
    const contributionStats = await getContributionStatsForYear(boundaries);
    const yearData = await buildYearSnapshot({
      year,
      viewer,
      organizations,
      identities,
      timezone,
      fetchRemotes,
      cloneMissingRepos,
      cacheDir,
      localRepoIndex,
      repoCatalog,
      contributionStats,
      boundaries,
      metadataCache,
      repoPathCache,
      defaultBranchProbeCache,
    });

    const outputPath = path.join(outputDir, `${year}.json`);
    await fs.writeFile(outputPath, JSON.stringify(yearData, null, 2));
    console.log(`Saved ${outputPath}`);
    yearSnapshots.push(yearData);
  }

  await writeSqliteSnapshot({
    sqlitePath,
    viewer,
    organizations,
    yearSnapshots,
  });
  console.log(`Saved ${sqlitePath}`);
}

function parseArgs(argv) {
  const args = {
    years: [],
    config: null,
    timezone: null,
    fetchRemotes: null,
    cloneMissingRepos: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--years") {
      while (argv[index + 1] && !argv[index + 1].startsWith("--")) {
        index += 1;
        args.years.push(Number(argv[index]));
      }
      continue;
    }

    if (value === "--config") {
      args.config = argv[index + 1];
      index += 1;
      continue;
    }

    if (value === "--timezone") {
      args.timezone = argv[index + 1];
      index += 1;
      continue;
    }

    if (value === "--fetch-remotes") {
      args.fetchRemotes = true;
      continue;
    }

    if (value === "--no-fetch-remotes") {
      args.fetchRemotes = false;
      continue;
    }

    if (value === "--clone-missing-repos") {
      args.cloneMissingRepos = true;
      continue;
    }

    if (value === "--no-clone-missing-repos") {
      args.cloneMissingRepos = false;
    }
  }

  return args;
}

function normalizePaths(paths) {
  return paths.map((entry) => path.resolve(entry));
}

async function run(command, args, options = {}) {
  const result = await execFile(command, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    maxBuffer: MAX_BUFFER,
  });

  return {
    stdout: result.stdout.trimEnd(),
    stderr: result.stderr.trimEnd(),
  };
}

async function ghGraphql(query, variables = {}) {
  const args = ["api", "graphql", "-f", `query=${query}`];
  for (const [key, value] of Object.entries(variables)) {
    if (value === null || value === undefined) {
      continue;
    }
    args.push("-F", `${key}=${value}`);
  }

  const { stdout } = await runWithRetries("gh", args);
  return JSON.parse(stdout);
}

async function ghRestJson(endpoint) {
  const { stdout } = await runWithRetries("gh", ["api", endpoint, "-H", "Accept: application/vnd.github+json"]);
  return JSON.parse(stdout);
}

async function runWithRetries(command, args, options = {}) {
  const maxAttempts = options.maxAttempts ?? 4;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await run(command, args, options);
    } catch (error) {
      const combined = `${error.stderr ?? ""}\n${error.stdout ?? ""}\n${error.message ?? ""}`;
      const retriable = /\bHTTP 5\d\d\b|502 Bad Gateway|ECONNRESET|ETIMEDOUT/i.test(combined);
      if (!retriable || attempt === maxAttempts) {
        throw error;
      }

      const delayMs = attempt * 1000;
      console.warn(`Retrying ${command} (${attempt}/${maxAttempts}) after transient error...`);
      await sleep(delayMs);
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function discoverLocalRepos(roots) {
  const repoCandidates = [];
  for (const root of roots) {
    try {
      await fs.access(root);
    } catch {
      continue;
    }

    const findArgs = [
      root,
      "(",
      "-path",
      "*/node_modules",
      "-o",
      "-path",
      "*/dist",
      "-o",
      "-path",
      "*/build",
      "-o",
      "-path",
      "*/.next",
      ")",
      "-prune",
      "-o",
      "-name",
      ".git",
      "(",
      "-type",
      "d",
      "-o",
      "-type",
      "f",
      ")",
      "-print",
    ];

    const { stdout } = await run("find", findArgs);
    const found = stdout
      .split("\n")
      .filter(Boolean)
      .map((entry) => path.dirname(entry));

    repoCandidates.push(...found);
  }

  const repoMap = new Map();
  for (const repoPath of [...new Set(repoCandidates)]) {
    try {
      const { stdout } = await run("git", ["-C", repoPath, "remote", "get-url", "origin"]);
      const slug = parseGitHubSlug(stdout);
      if (!slug) {
        continue;
      }

      const list = repoMap.get(slug) ?? [];
      list.push(repoPath);
      repoMap.set(slug, list);
    } catch {
      // Ignore repos without an origin remote.
    }
  }

  const bestMatch = new Map();
  for (const [slug, repoPaths] of repoMap.entries()) {
    const preferred = [...repoPaths].sort(compareRepoPaths)[0];
    bestMatch.set(slug, {
      primaryPath: preferred,
      allPaths: [...repoPaths].sort(compareRepoPaths),
    });
  }

  console.log(`Matched ${bestMatch.size} local GitHub repositories.`);
  return bestMatch;
}

function compareRepoPaths(left, right) {
  const leftWorktree = Number(left.includes("/worktrees/"));
  const rightWorktree = Number(right.includes("/worktrees/"));
  if (leftWorktree !== rightWorktree) {
    return leftWorktree - rightWorktree;
  }
  return left.length - right.length;
}

function parseGitHubSlug(remoteUrl) {
  const normalized = remoteUrl.trim();
  const match =
    normalized.match(/github\.com[:/](.+?)(?:\.git)?$/i) ??
    normalized.match(/^git@github\.com:(.+?)(?:\.git)?$/i);
  return match?.[1] ?? null;
}

async function getViewer() {
  const query = `
    query {
      viewer {
        login
        name
        databaseId
        id
        url
      }
    }
  `;

  const response = await ghGraphql(query);
  return response.data.viewer;
}

async function getOrganizations() {
  const response = await ghRestJson("/user/orgs?per_page=100");
  return response.map((entry) => ({
    login: entry.login,
    id: entry.id,
  }));
}

function buildAuthorIdentities(viewer) {
  const databaseEmail = `${viewer.databaseId}+${viewer.login}@users.noreply.github.com`;
  const simpleEmail = `${viewer.login}@users.noreply.github.com`;
  const values = new Set([
    viewer.login,
    viewer.name,
    "Denis Kiselev",
    "deksden@deksden.com",
    databaseEmail,
    simpleEmail,
  ]);

  return [...values].filter(Boolean);
}

function buildYearBoundaries(year) {
  const now = new Date();
  const isCurrentYear = year === now.getFullYear();
  return {
    since: new Date(year, 0, 1, 0, 0, 0, 0),
    until: isCurrentYear ? now : new Date(year, 11, 31, 23, 59, 59, 999),
    isCurrentYear,
  };
}

async function getContributionStatsForYear(boundaries) {
  const query = `
    query($from: DateTime!, $to: DateTime!) {
      viewer {
        contributionsCollection(from: $from, to: $to) {
          totalCommitContributions
          commitContributionsByRepository(maxRepositories: 100) {
            repository {
              nameWithOwner
              isPrivate
            }
          }
        }
      }
    }
  `;

  const response = await ghGraphql(query, {
    from: boundaries.since.toISOString(),
    to: boundaries.until.toISOString(),
  });

  return {
    totalCommitContributions: response.data.viewer.contributionsCollection.totalCommitContributions,
    contributionRepositories:
      response.data.viewer.contributionsCollection.commitContributionsByRepository.map((entry) => entry.repository),
  };
}

async function getRepositoryCatalog({ viewer, organizations, config, localRepoIndex }) {
  const ownerLogins = [viewer.login, ...organizations.map((entry) => entry.login)];
  const includeRepos = new Set(config.includeRepos ?? []);
  const excludeRepos = new Set(config.excludeRepos ?? []);
  const ownerSet = new Set(ownerLogins);
  const catalog = new Map();

  console.log(`Discovering repositories for owners: ${ownerLogins.join(", ")}`);

  for (const owner of ownerLogins) {
    const repositories = await listOwnerRepositories(owner);
    for (const repository of repositories) {
      if (!excludeRepos.has(repository.nameWithOwner)) {
        catalog.set(repository.nameWithOwner, repository);
      }
    }
  }

  for (const [slug] of localRepoIndex.entries()) {
    const owner = slug.split("/")[0];
    if (ownerSet.has(owner) && !excludeRepos.has(slug) && !catalog.has(slug)) {
      catalog.set(slug, {
        nameWithOwner: slug,
        isPrivate: null,
        isArchived: false,
        pushedAt: null,
      });
    }
  }

  for (const slug of includeRepos) {
    if (!excludeRepos.has(slug) && !catalog.has(slug)) {
      catalog.set(slug, {
        nameWithOwner: slug,
        isPrivate: null,
        isArchived: false,
        pushedAt: null,
      });
    }
  }

  const repositories = [...catalog.values()].sort((left, right) => left.nameWithOwner.localeCompare(right.nameWithOwner));
  console.log(`Catalog contains ${repositories.length} candidate repositories.`);
  return {
    owners: ownerLogins,
    organizations: organizations.map((entry) => entry.login),
    repositories,
  };
}

async function listOwnerRepositories(owner) {
  const { stdout } = await run("gh", [
    "repo",
    "list",
    owner,
    "--limit",
    "200",
    "--json",
    "nameWithOwner,isPrivate,isArchived,pushedAt",
  ]);

  return JSON.parse(stdout);
}

async function buildYearSnapshot(context) {
  const {
    year,
    viewer,
    organizations,
    identities,
    timezone,
    fetchRemotes,
    cloneMissingRepos,
    cacheDir,
    localRepoIndex,
    repoCatalog,
    contributionStats,
    boundaries,
    metadataCache,
    repoPathCache,
    defaultBranchProbeCache,
  } = context;

  const projectEntries = [];
  let examinedRepositories = 0;
  let skippedByPushedAt = 0;
  let skippedByProbe = 0;
  const contributionRepoSet = new Set(
    contributionStats.contributionRepositories.map((entry) => entry.nameWithOwner),
  );

  for (const repoEntry of repoCatalog.repositories) {
    const slug = repoEntry.nameWithOwner;
    if (repoEntry.pushedAt && new Date(repoEntry.pushedAt) < boundaries.since) {
      skippedByPushedAt += 1;
      continue;
    }

    examinedRepositories += 1;
    console.log(`  • ${slug}`);
    const localInfo = localRepoIndex.get(slug) ?? null;

    if (!localInfo?.primaryPath && !contributionRepoSet.has(slug)) {
      const defaultBranchCommitCount = await probeDefaultBranchCommitCount({
        slug,
        authorId: viewer.id,
        boundaries,
        defaultBranchProbeCache,
      });

      if (defaultBranchCommitCount === 0) {
        skippedByProbe += 1;
        console.warn(`    No default-branch matches for ${slug}; skipping clone.`);
        continue;
      }
    }

    const repoPath = await ensureRepoPath({
      slug,
      localInfo,
      cacheDir,
      fetchRemotes,
      cloneMissingRepos,
      repoPathCache,
    });

    const metadata = await getRepositoryMetadata(slug, metadataCache);
    const gitStats = repoPath
      ? await analyzeRepositoryFromGit({
          repoPath,
          slug,
          identities,
          boundaries,
          timezone,
        })
      : emptyGitStats();

    const fallbackCommits =
      gitStats.commits.length === 0
        ? await getFallbackCommitsFromGraphql({
            slug,
            authorId: viewer.id,
            boundaries,
          })
        : [];

    const commitStats = gitStats.commits.length > 0 ? gitStats : buildGitStatsFromCommits(fallbackCommits, timezone);

    if (commitStats.commits.length === 0) {
      console.warn(`    No commits matched for ${slug}; skipping.`);
      continue;
    }

    const workflows = await getWorkflowStats(slug, viewer.login, boundaries, timezone);
    projectEntries.push(
      buildProjectStats({
        slug,
        metadata,
        repoPath,
        commitStats,
        workflows,
        timezone,
      }),
    );
  }

  const yearSummary = buildYearSummary(projectEntries, year, timezone, boundaries, contributionStats.totalCommitContributions);

  return {
    year,
    generatedAt: new Date().toISOString(),
    timezone,
    viewer: {
      login: viewer.login,
      name: viewer.name,
      url: viewer.url,
    },
    methodology: {
      repositoryDiscovery:
        "All repositories from the viewer account, all accessible organizations, and matching local GitHub remotes, with yearly inclusion only when author-matched commits exist.",
      commitSource:
        "Local git clone with `git log --all --numstat` when available, otherwise fallback to GitHub GraphQL history on the default branch.",
      languageMethod:
        "GitHub Linguist repository language mix weighted by yearly line churn (additions + deletions) in the repository.",
      workflowMethod:
        "GitHub Actions workflow runs filtered by actor login and created_at inside the selected year range.",
    },
    scope: {
      viewerLogin: viewer.login,
      organizations: organizations.map((entry) => entry.login),
      repositoryOwners: repoCatalog.owners,
      candidateRepositories: repoCatalog.repositories.length,
      examinedRepositories,
      skippedByPushedAt,
      skippedByProbe,
      contributionGraphRepositories: contributionStats.contributionRepositories.map((entry) => entry.nameWithOwner),
    },
    yearRange: {
      since: boundaries.since.toISOString(),
      until: boundaries.until.toISOString(),
      isCurrentYear: boundaries.isCurrentYear,
    },
    summary: yearSummary.summary,
    chronology: yearSummary.chronology,
    months: yearSummary.months,
    weekdays: yearSummary.weekdays,
    hours: yearSummary.hours,
    languages: yearSummary.languages,
    workflows: yearSummary.workflows,
    projects: projectEntries,
  };
}

async function ensureRepoPath({ slug, localInfo, cacheDir, fetchRemotes, cloneMissingRepos, repoPathCache }) {
  const cached = repoPathCache.get(slug);
  if (cached) {
    return cached;
  }

  if (localInfo?.primaryPath) {
    if (fetchRemotes) {
      try {
        await run("git", ["-C", localInfo.primaryPath, "fetch", "--all", "--prune", "--quiet"]);
      } catch (error) {
        console.warn(`    fetch skipped for ${slug}: ${error.message}`);
      }
    }
    repoPathCache.set(slug, localInfo.primaryPath);
    return localInfo.primaryPath;
  }

  if (!cloneMissingRepos) {
    return null;
  }

  const repoDirName = slug.replaceAll("/", "__");
  const targetPath = path.join(cacheDir, repoDirName);

  try {
    await fs.access(targetPath);
    if (fetchRemotes) {
      await run("git", ["-C", targetPath, "fetch", "--all", "--prune", "--quiet"]);
    }
    repoPathCache.set(slug, targetPath);
    return targetPath;
  } catch {
    console.log(`    cloning ${slug} into cache...`);
  }

  try {
    await run("gh", ["repo", "clone", slug, targetPath]);
    repoPathCache.set(slug, targetPath);
    return targetPath;
  } catch (error) {
    console.warn(`    clone failed for ${slug}: ${error.message}`);
    return null;
  }
}

async function getRepositoryMetadata(slug, metadataCache) {
  const cached = metadataCache.get(slug);
  if (cached) {
    return cached;
  }

  const [owner, name] = slug.split("/");
  const query = `
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        name
        nameWithOwner
        url
        description
        homepageUrl
        isPrivate
        isArchived
        stargazerCount
        forkCount
        createdAt
        updatedAt
        pushedAt
        owner {
          login
        }
        languages(first: 10, orderBy: {field: SIZE, direction: DESC}) {
          totalSize
          edges {
            size
            node {
              name
              color
            }
          }
        }
      }
    }
  `;

  const response = await ghGraphql(query, { owner, name });
  metadataCache.set(slug, response.data.repository);
  return response.data.repository;
}

async function analyzeRepositoryFromGit({ repoPath, slug, identities, boundaries, timezone }) {
  const authorRegex = identities
    .map((entry) => escapeRegex(entry))
    .sort((left, right) => right.length - left.length)
    .join("|");

  const args = [
    "-C",
    repoPath,
    "log",
    "--all",
    "--use-mailmap",
    "--numstat",
    `--since=${boundaries.since.toISOString()}`,
    `--until=${boundaries.until.toISOString()}`,
    "--date=iso-strict",
    `--author=${authorRegex}`,
    `--pretty=format:${COMMIT_MARKER}%n%H%x09%aI%x09%cI%x09%an%x09%ae%x09%s`,
  ];

  try {
    const { stdout } = await run("git", args);
    const commits = parseGitLog(stdout, timezone);
    return buildGitStatsFromCommits(commits, timezone);
  } catch (error) {
    console.warn(`    git analysis failed for ${slug}: ${error.message}`);
    return emptyGitStats();
  }
}

function parseGitLog(output, timezone) {
  if (!output.trim()) {
    return [];
  }

  const lines = output.split("\n");
  const commits = [];
  let current = null;

  for (const line of lines) {
    if (line === COMMIT_MARKER) {
      if (current) {
        commits.push(finalizeCommit(current, timezone));
      }
      current = null;
      continue;
    }

    if (!current) {
      const [sha, authoredDate, committedDate, authorName, authorEmail, ...subjectRest] = line.split("\t");
      current = {
        sha,
        authoredDate,
        committedDate,
        authorName,
        authorEmail,
        subject: subjectRest.join("\t"),
        additions: 0,
        deletions: 0,
        changedFiles: 0,
      };
      continue;
    }

    if (!line.trim()) {
      continue;
    }

    const [additionsRaw, deletionsRaw] = line.split("\t");
    if (additionsRaw === undefined || deletionsRaw === undefined) {
      continue;
    }

    current.changedFiles += 1;
    if (additionsRaw !== "-") {
      current.additions += Number(additionsRaw) || 0;
    }
    if (deletionsRaw !== "-") {
      current.deletions += Number(deletionsRaw) || 0;
    }
  }

  if (current) {
    commits.push(finalizeCommit(current, timezone));
  }

  return commits.filter((commit) => commit.sha);
}

function finalizeCommit(commit, timezone) {
  const activityDate = commit.authoredDate || commit.committedDate;
  return {
    ...commit,
    activityDate,
    dayKey: formatInTimezone(activityDate, timezone, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }),
    monthKey: formatInTimezone(activityDate, timezone, {
      year: "numeric",
      month: "2-digit",
    }),
    hour: Number(
      formatInTimezone(activityDate, timezone, {
        hour: "2-digit",
        hourCycle: "h23",
      }),
    ),
    weekday: formatInTimezone(activityDate, timezone, {
      weekday: "short",
    }),
  };
}

function buildGitStatsFromCommits(commits, timezone) {
  if (!commits.length) {
    return emptyGitStats();
  }

  const normalizedCommits = commits.map((commit) =>
    commit.dayKey ? commit : finalizeCommit(commit, timezone),
  );
  const chronology = new Map();
  const months = new Map();
  const weekdays = initWeekdayMap();
  const hours = initHourMap();

  for (const commit of normalizedCommits) {
    const dayBucket = chronology.get(commit.dayKey) ?? {
      date: commit.dayKey,
      commits: 0,
      additions: 0,
      deletions: 0,
    };
    dayBucket.commits += 1;
    dayBucket.additions += commit.additions;
    dayBucket.deletions += commit.deletions;
    chronology.set(commit.dayKey, dayBucket);

    const monthBucket = months.get(commit.monthKey) ?? {
      month: commit.monthKey,
      commits: 0,
      additions: 0,
      deletions: 0,
      days: new Set(),
    };
    monthBucket.commits += 1;
    monthBucket.additions += commit.additions;
    monthBucket.deletions += commit.deletions;
    monthBucket.days.add(commit.dayKey);
    months.set(commit.monthKey, monthBucket);

    const weekdayBucket = weekdays.get(commit.weekday) ?? weekdays.get("Mon");
    weekdayBucket.commits += 1;
    weekdayBucket.additions += commit.additions;
    weekdayBucket.deletions += commit.deletions;

    const hourBucket = hours.get(Number.isInteger(commit.hour) ? commit.hour : 0);
    hourBucket.commits += 1;
    hourBucket.additions += commit.additions;
    hourBucket.deletions += commit.deletions;
  }

  const activeDays = [...chronology.keys()].sort();
  const additions = normalizedCommits.reduce((sum, commit) => sum + commit.additions, 0);
  const deletions = normalizedCommits.reduce((sum, commit) => sum + commit.deletions, 0);
  const changedFiles = normalizedCommits.reduce((sum, commit) => sum + commit.changedFiles, 0);
  const sortedCommits = [...normalizedCommits].sort((left, right) => left.activityDate.localeCompare(right.activityDate));

  return {
    commits: sortedCommits,
    totals: {
      commits: normalizedCommits.length,
      activeDays: activeDays.length,
      additions,
      deletions,
      netLines: additions - deletions,
      changedFiles,
      longestStreak: calculateLongestStreak(activeDays),
      firstCommitAt: sortedCommits[0]?.activityDate ?? null,
      lastCommitAt: sortedCommits.at(-1)?.activityDate ?? null,
    },
    chronology: [...chronology.values()].sort((left, right) => left.date.localeCompare(right.date)),
    months: [...months.values()]
      .map((entry) => ({
        month: entry.month,
        commits: entry.commits,
        additions: entry.additions,
        deletions: entry.deletions,
        activeDays: entry.days.size,
      }))
      .sort((left, right) => left.month.localeCompare(right.month)),
    weekdays: WEEKDAY_ORDER.map((label) => ({ weekday: label, ...weekdays.get(label) })),
    hours: [...hours.values()],
  };
}

function emptyGitStats() {
  return {
    commits: [],
    totals: {
      commits: 0,
      activeDays: 0,
      additions: 0,
      deletions: 0,
      netLines: 0,
      changedFiles: 0,
      longestStreak: 0,
      firstCommitAt: null,
      lastCommitAt: null,
    },
    chronology: [],
    months: [],
    weekdays: WEEKDAY_ORDER.map((weekday) => ({
      weekday,
      commits: 0,
      additions: 0,
      deletions: 0,
    })),
    hours: Array.from({ length: 24 }, (_, hour) => ({
      hour,
      commits: 0,
      additions: 0,
      deletions: 0,
    })),
  };
}

async function getFallbackCommitsFromGraphql({ slug, authorId, boundaries }) {
  const [owner, name] = slug.split("/");
  const query = `
    query($owner: String!, $name: String!, $authorId: ID!, $since: GitTimestamp!, $until: GitTimestamp!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(first: 100, after: $cursor, since: $since, until: $until, author: {id: $authorId}) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  oid
                  authoredDate
                  committedDate
                  additions
                  deletions
                  changedFilesIfAvailable
                  messageHeadline
                  author {
                    name
                    email
                  }
                }
              }
            }
          }
        }
      }
    }
  `;

  const commits = [];
  let cursor = null;

  while (true) {
    const response = await ghGraphql(query, {
      owner,
      name,
      authorId,
      since: boundaries.since.toISOString(),
      until: boundaries.until.toISOString(),
      cursor,
    });

    const history =
      response.data.repository?.defaultBranchRef?.target?.history ?? null;

    if (!history) {
      break;
    }

    for (const node of history.nodes) {
      commits.push({
        sha: node.oid,
        authoredDate: node.authoredDate,
        committedDate: node.committedDate,
        authorName: node.author?.name ?? "",
        authorEmail: node.author?.email ?? "",
        subject: node.messageHeadline ?? "",
        additions: node.additions ?? 0,
        deletions: node.deletions ?? 0,
        changedFiles: node.changedFilesIfAvailable ?? 0,
      });
    }

    if (!history.pageInfo.hasNextPage) {
      break;
    }
    cursor = history.pageInfo.endCursor;
  }

  return commits;
}

async function probeDefaultBranchCommitCount({ slug, authorId, boundaries, defaultBranchProbeCache }) {
  const cacheKey = `${slug}:${boundaries.since.toISOString()}:${boundaries.until.toISOString()}`;
  if (defaultBranchProbeCache.has(cacheKey)) {
    return defaultBranchProbeCache.get(cacheKey);
  }

  const [owner, name] = slug.split("/");
  const query = `
    query($owner: String!, $name: String!, $authorId: ID!, $since: GitTimestamp!, $until: GitTimestamp!) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(first: 1, since: $since, until: $until, author: {id: $authorId}) {
                totalCount
              }
            }
          }
        }
      }
    }
  `;

  try {
    const response = await ghGraphql(query, {
      owner,
      name,
      authorId,
      since: boundaries.since.toISOString(),
      until: boundaries.until.toISOString(),
    });

    const totalCount =
      response.data.repository?.defaultBranchRef?.target?.history?.totalCount ?? 0;
    defaultBranchProbeCache.set(cacheKey, totalCount);
    return totalCount;
  } catch (error) {
    console.warn(`    default-branch probe failed for ${slug}: ${error.message}`);
    defaultBranchProbeCache.set(cacheKey, null);
    return null;
  }
}

async function getWorkflowStats(slug, actor, boundaries, timezone) {
  const createdRange = `${boundaries.since.toISOString().slice(0, 10)}..${boundaries.until.toISOString().slice(0, 10)}`;
  const runs = [];

  for (let page = 1; page <= 20; page += 1) {
    const endpoint = `/repos/${slug}/actions/runs?per_page=100&page=${page}&actor=${encodeURIComponent(
      actor,
    )}&created=${encodeURIComponent(createdRange)}`;

    try {
      const response = await ghRestJson(endpoint);
      const pageRuns = response.workflow_runs ?? [];
      runs.push(...pageRuns);

      if (pageRuns.length < 100) {
        break;
      }
    } catch (error) {
      return {
        total: 0,
        success: 0,
        failure: 0,
        cancelled: 0,
        skipped: 0,
        other: 0,
        byMonth: [],
        byEvent: [],
        byWorkflow: [],
        unavailable: true,
        error: error.message,
      };
    }
  }

  const byMonth = new Map();
  const byEvent = new Map();
  const byWorkflow = new Map();
  let success = 0;
  let failure = 0;
  let cancelled = 0;
  let skipped = 0;
  let other = 0;

  for (const runEntry of runs) {
    const createdAt = runEntry.run_started_at || runEntry.created_at;
    const month = formatInTimezone(createdAt, timezone, {
      year: "numeric",
      month: "2-digit",
    });
    byMonth.set(month, (byMonth.get(month) ?? 0) + 1);
    byEvent.set(runEntry.event || "unknown", (byEvent.get(runEntry.event || "unknown") ?? 0) + 1);
    byWorkflow.set(runEntry.name || "Unnamed workflow", (byWorkflow.get(runEntry.name || "Unnamed workflow") ?? 0) + 1);

    switch (runEntry.conclusion) {
      case "success":
        success += 1;
        break;
      case "failure":
      case "startup_failure":
      case "timed_out":
        failure += 1;
        break;
      case "cancelled":
        cancelled += 1;
        break;
      case "skipped":
        skipped += 1;
        break;
      default:
        other += 1;
        break;
    }
  }

  return {
    total: runs.length,
    success,
    failure,
    cancelled,
    skipped,
    other,
    unavailable: false,
    byMonth: [...byMonth.entries()]
      .map(([month, count]) => ({ month, count }))
      .sort((left, right) => left.month.localeCompare(right.month)),
    byEvent: [...byEvent.entries()]
      .map(([event, count]) => ({ event, count }))
      .sort((left, right) => right.count - left.count),
    byWorkflow: [...byWorkflow.entries()]
      .map(([workflow, count]) => ({ workflow, count }))
      .sort((left, right) => right.count - left.count)
      .slice(0, 10),
  };
}

function buildProjectStats({ slug, metadata, repoPath, commitStats, workflows, timezone }) {
  const lineWeight = commitStats.totals.additions + commitStats.totals.deletions;
  const languageTotalSize = metadata.languages?.totalSize ?? 0;
  const languages = (metadata.languages?.edges ?? []).map((edge) => {
    const ratio = languageTotalSize > 0 ? edge.size / languageTotalSize : 0;
    return {
      name: edge.node.name,
      color: edge.node.color,
      bytes: edge.size,
      ratio,
      estimatedWeightedLines: Math.round(lineWeight * ratio),
    };
  });

  return {
    id: slug,
    owner: metadata.owner.login,
    name: metadata.name,
    displayName: metadata.name,
    visibility: metadata.isPrivate ? "private" : "public",
    isPrivate: metadata.isPrivate,
    isArchived: metadata.isArchived,
    url: metadata.url,
    description: metadata.description,
    homepageUrl: metadata.homepageUrl,
    repoPath,
    stars: metadata.stargazerCount,
    forks: metadata.forkCount,
    createdAt: metadata.createdAt,
    updatedAt: metadata.updatedAt,
    pushedAt: metadata.pushedAt,
    metrics: {
      commits: commitStats.totals.commits,
      activeDays: commitStats.totals.activeDays,
      longestStreak: commitStats.totals.longestStreak,
      additions: commitStats.totals.additions,
      deletions: commitStats.totals.deletions,
      netLines: commitStats.totals.netLines,
      changedFiles: commitStats.totals.changedFiles,
      firstCommitAt: commitStats.totals.firstCommitAt,
      lastCommitAt: commitStats.totals.lastCommitAt,
      averageCommitsPerActiveDay:
        commitStats.totals.activeDays > 0
          ? round(commitStats.totals.commits / commitStats.totals.activeDays, 2)
          : 0,
    },
    chronology: commitStats.chronology,
    months: commitStats.months,
    weekdays: commitStats.weekdays,
    hours: commitStats.hours,
    languages,
    workflows,
    source: {
      timezone,
      usedLocalGit: Boolean(repoPath),
    },
  };
}

function buildYearSummary(projects, year, timezone, boundaries, contributionCount) {
  const chronology = new Map();
  const months = initMonthMap(year);
  const weekdays = initWeekdayMap();
  const hours = initHourMap();
  const languages = new Map();
  const monthDaySets = new Map([...months.keys()].map((month) => [month, new Set()]));
  const workflowMonths = initMonthMap(year);
  const workflowEvents = new Map();
  const activeDays = new Set();

  let commits = 0;
  let additions = 0;
  let deletions = 0;
  let workflowTotal = 0;
  let workflowSuccess = 0;
  let workflowFailure = 0;
  let workflowCancelled = 0;
  let workflowSkipped = 0;
  let workflowOther = 0;

  for (const project of projects) {
    commits += project.metrics.commits;
    additions += project.metrics.additions;
    deletions += project.metrics.deletions;

    for (const bucket of project.chronology) {
      const existing = chronology.get(bucket.date) ?? {
        date: bucket.date,
        commits: 0,
        additions: 0,
        deletions: 0,
      };
      existing.commits += bucket.commits;
      existing.additions += bucket.additions;
      existing.deletions += bucket.deletions;
      chronology.set(bucket.date, existing);
      activeDays.add(bucket.date);
    }

    for (const bucket of project.months) {
      const monthEntry =
        months.get(bucket.month) ??
        {
          month: bucket.month,
          commits: 0,
          additions: 0,
          deletions: 0,
          activeDays: 0,
          workflowRuns: 0,
        };
      months.set(bucket.month, monthEntry);
      monthEntry.commits += bucket.commits;
      monthEntry.additions += bucket.additions;
      monthEntry.deletions += bucket.deletions;
    }

    for (const bucket of project.chronology) {
      const monthKey = bucket.date.slice(0, 7);
      const monthDays = monthDaySets.get(monthKey);
      if (monthDays) {
        monthDays.add(bucket.date);
      }
    }

    for (const bucket of project.weekdays) {
      const weekdayEntry = weekdays.get(bucket.weekday);
      weekdayEntry.commits += bucket.commits;
      weekdayEntry.additions += bucket.additions;
      weekdayEntry.deletions += bucket.deletions;
    }

    for (const bucket of project.hours) {
      const hourEntry = hours.get(bucket.hour);
      hourEntry.commits += bucket.commits;
      hourEntry.additions += bucket.additions;
      hourEntry.deletions += bucket.deletions;
    }

    for (const language of project.languages) {
      const entry = languages.get(language.name) ?? {
        name: language.name,
        color: language.color,
        weightedLines: 0,
        repoCount: 0,
      };
      entry.weightedLines += language.estimatedWeightedLines;
      entry.repoCount += 1;
      languages.set(language.name, entry);
    }

    workflowTotal += project.workflows.total;
    workflowSuccess += project.workflows.success;
    workflowFailure += project.workflows.failure;
    workflowCancelled += project.workflows.cancelled;
    workflowSkipped += project.workflows.skipped;
    workflowOther += project.workflows.other;

    for (const bucket of project.workflows.byMonth) {
      const monthEntry =
        workflowMonths.get(bucket.month) ??
        {
          month: bucket.month,
          commits: 0,
          additions: 0,
          deletions: 0,
          activeDays: 0,
          workflowRuns: 0,
        };
      workflowMonths.set(bucket.month, monthEntry);
      monthEntry.workflowRuns += bucket.count;
    }
    for (const bucket of project.workflows.byEvent) {
      workflowEvents.set(bucket.event, (workflowEvents.get(bucket.event) ?? 0) + bucket.count);
    }
  }

  const chronologyList = [...chronology.values()].sort((left, right) => left.date.localeCompare(right.date));
  const busiestDay = chronologyList.toSorted((left, right) => right.commits - left.commits)[0] ?? null;
  for (const [month, daySet] of monthDaySets.entries()) {
    months.get(month).activeDays = daySet.size;
  }

  return {
    summary: {
      projectsWorkedOn: projects.length,
      publicProjects: projects.filter((project) => !project.isPrivate).length,
      privateProjects: projects.filter((project) => project.isPrivate).length,
      commits,
      discoveredCommitContributions: contributionCount,
      activeDays: activeDays.size,
      longestStreak: calculateLongestStreak([...activeDays].sort()),
      additions,
      deletions,
      netLines: additions - deletions,
      averageCommitsPerActiveDay: activeDays.size > 0 ? round(commits / activeDays.size, 2) : 0,
      workflowRuns: workflowTotal,
      workflowSuccess,
      workflowFailure,
      workflowCancelled,
      workflowSkipped,
      workflowOther,
      busiestDay,
      busiestMonth: [...months.values()].toSorted((left, right) => right.commits - left.commits)[0] ?? null,
      yearStartedAt: boundaries.since.toISOString(),
      yearEndedAt: boundaries.until.toISOString(),
      timezone,
    },
    chronology: chronologyList,
    months: [...months.values()],
    weekdays: WEEKDAY_ORDER.map((label) => ({ weekday: label, ...weekdays.get(label) })),
    hours: [...hours.values()],
    languages: [...languages.values()].sort((left, right) => right.weightedLines - left.weightedLines),
    workflows: {
      total: workflowTotal,
      success: workflowSuccess,
      failure: workflowFailure,
      cancelled: workflowCancelled,
      skipped: workflowSkipped,
      other: workflowOther,
      byMonth: [...workflowMonths.values()],
      byEvent: [...workflowEvents.entries()]
        .map(([event, count]) => ({ event, count }))
        .sort((left, right) => right.count - left.count),
    },
  };
}

function initMonthMap(year) {
  const map = new Map();
  for (let month = 1; month <= 12; month += 1) {
    const key = `${year}-${String(month).padStart(2, "0")}`;
    map.set(key, {
      month: key,
      commits: 0,
      additions: 0,
      deletions: 0,
      activeDays: 0,
      workflowRuns: 0,
    });
  }
  return map;
}

function initWeekdayMap() {
  return new Map(
    WEEKDAY_ORDER.map((weekday) => [
      weekday,
      {
        commits: 0,
        additions: 0,
        deletions: 0,
      },
    ]),
  );
}

function initHourMap() {
  return new Map(
    Array.from({ length: 24 }, (_, hour) => [
      hour,
      {
        hour,
        commits: 0,
        additions: 0,
        deletions: 0,
      },
    ]),
  );
}

function calculateLongestStreak(sortedDayKeys) {
  if (!sortedDayKeys.length) {
    return 0;
  }

  let current = 1;
  let longest = 1;

  for (let index = 1; index < sortedDayKeys.length; index += 1) {
    const previous = new Date(`${sortedDayKeys[index - 1]}T00:00:00Z`);
    const next = new Date(`${sortedDayKeys[index]}T00:00:00Z`);
    const diffDays = Math.round((next - previous) / 86400000);
    if (diffDays === 1) {
      current += 1;
      longest = Math.max(longest, current);
    } else {
      current = 1;
    }
  }

  return longest;
}

function formatInTimezone(dateInput, timezone, options) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    ...options,
  }).format(new Date(dateInput));
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function round(value, digits = 0) {
  return Number(value.toFixed(digits));
}

export async function writeSqliteSnapshot({ sqlitePath, viewer, organizations, yearSnapshots }) {
  await fs.rm(sqlitePath, { force: true });
  const database = new DatabaseSync(sqlitePath);

  try {
    database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE years (
        year INTEGER PRIMARY KEY,
        generated_at TEXT NOT NULL,
        timezone TEXT NOT NULL,
        since_utc TEXT NOT NULL,
        until_utc TEXT NOT NULL,
        is_current_year INTEGER NOT NULL,
        projects_worked_on INTEGER NOT NULL,
        public_projects INTEGER NOT NULL,
        private_projects INTEGER NOT NULL,
        commits INTEGER NOT NULL,
        discovered_commit_contributions INTEGER NOT NULL,
        active_days INTEGER NOT NULL,
        longest_streak INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        net_lines INTEGER NOT NULL,
        average_commits_per_active_day REAL NOT NULL,
        workflow_runs INTEGER NOT NULL,
        workflow_success INTEGER NOT NULL,
        workflow_failure INTEGER NOT NULL,
        workflow_cancelled INTEGER NOT NULL,
        workflow_skipped INTEGER NOT NULL,
        workflow_other INTEGER NOT NULL,
        busiest_day_date TEXT,
        busiest_day_commits INTEGER,
        busiest_month TEXT,
        busiest_month_commits INTEGER,
        scope_json TEXT NOT NULL
      );

      CREATE TABLE projects (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        owner TEXT NOT NULL,
        name TEXT NOT NULL,
        display_name TEXT NOT NULL,
        visibility TEXT NOT NULL,
        is_private INTEGER NOT NULL,
        is_archived INTEGER NOT NULL,
        url TEXT,
        description TEXT,
        homepage_url TEXT,
        repo_path TEXT,
        stars INTEGER NOT NULL,
        forks INTEGER NOT NULL,
        created_at TEXT,
        updated_at TEXT,
        pushed_at TEXT,
        commits INTEGER NOT NULL,
        active_days INTEGER NOT NULL,
        longest_streak INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        net_lines INTEGER NOT NULL,
        changed_files INTEGER NOT NULL,
        first_commit_at TEXT,
        last_commit_at TEXT,
        average_commits_per_active_day REAL NOT NULL,
        workflow_total INTEGER NOT NULL,
        workflow_success INTEGER NOT NULL,
        workflow_failure INTEGER NOT NULL,
        workflow_cancelled INTEGER NOT NULL,
        workflow_skipped INTEGER NOT NULL,
        workflow_other INTEGER NOT NULL,
        source_json TEXT NOT NULL,
        PRIMARY KEY (year, project_id),
        FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE
      );

      CREATE TABLE project_languages (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        language_name TEXT NOT NULL,
        color TEXT,
        bytes INTEGER NOT NULL,
        ratio REAL NOT NULL,
        estimated_weighted_lines INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, language_name),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE project_chronology (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        date TEXT NOT NULL,
        commits INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, date),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE project_months (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        month TEXT NOT NULL,
        commits INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        active_days INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, month),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE project_weekdays (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        weekday TEXT NOT NULL,
        commits INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, weekday),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE project_hours (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        hour INTEGER NOT NULL,
        commits INTEGER NOT NULL,
        additions INTEGER NOT NULL,
        deletions INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, hour),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE workflow_events (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        event TEXT NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, event),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );

      CREATE TABLE workflow_months (
        year INTEGER NOT NULL,
        project_id TEXT NOT NULL,
        month TEXT NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY (year, project_id, month),
        FOREIGN KEY (year, project_id) REFERENCES projects(year, project_id) ON DELETE CASCADE
      );
    `);

    const insertMetadata = database.prepare(`INSERT INTO metadata (key, value) VALUES (?, ?)`);
    const insertYear = database.prepare(`
      INSERT INTO years (
        year, generated_at, timezone, since_utc, until_utc, is_current_year,
        projects_worked_on, public_projects, private_projects, commits,
        discovered_commit_contributions, active_days, longest_streak, additions, deletions,
        net_lines, average_commits_per_active_day, workflow_runs, workflow_success,
        workflow_failure, workflow_cancelled, workflow_skipped, workflow_other,
        busiest_day_date, busiest_day_commits, busiest_month, busiest_month_commits, scope_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertProject = database.prepare(`
      INSERT INTO projects (
        year, project_id, owner, name, display_name, visibility, is_private, is_archived,
        url, description, homepage_url, repo_path, stars, forks, created_at, updated_at,
        pushed_at, commits, active_days, longest_streak, additions, deletions, net_lines,
        changed_files, first_commit_at, last_commit_at, average_commits_per_active_day,
        workflow_total, workflow_success, workflow_failure, workflow_cancelled, workflow_skipped,
        workflow_other, source_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertProjectLanguage = database.prepare(`
      INSERT INTO project_languages (
        year, project_id, language_name, color, bytes, ratio, estimated_weighted_lines
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    const insertProjectChronology = database.prepare(`
      INSERT INTO project_chronology (year, project_id, date, commits, additions, deletions)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const insertProjectMonth = database.prepare(`
      INSERT INTO project_months (year, project_id, month, commits, additions, deletions, active_days)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    const insertProjectWeekday = database.prepare(`
      INSERT INTO project_weekdays (year, project_id, weekday, commits, additions, deletions)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const insertProjectHour = database.prepare(`
      INSERT INTO project_hours (year, project_id, hour, commits, additions, deletions)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const insertWorkflowEvent = database.prepare(`
      INSERT INTO workflow_events (year, project_id, event, count) VALUES (?, ?, ?, ?)
    `);
    const insertWorkflowMonth = database.prepare(`
      INSERT INTO workflow_months (year, project_id, month, count) VALUES (?, ?, ?, ?)
    `);

    insertMetadata.run("dashboard_name", viewer.login);
    insertMetadata.run("viewer_login", viewer.login);
    insertMetadata.run("viewer_name", viewer.name ?? "");
    insertMetadata.run("viewer_url", viewer.url ?? "");
    insertMetadata.run("organizations", JSON.stringify(organizations.map((entry) => entry.login)));
    insertMetadata.run("generated_at", new Date().toISOString());

    database.exec("BEGIN");
    try {
      for (const yearData of yearSnapshots) {
        const summary = yearData.summary;
        insertYear.run(
          yearData.year,
          yearData.generatedAt,
          yearData.timezone,
          yearData.yearRange.since,
          yearData.yearRange.until,
          yearData.yearRange.isCurrentYear ? 1 : 0,
          summary.projectsWorkedOn,
          summary.publicProjects,
          summary.privateProjects,
          summary.commits,
          summary.discoveredCommitContributions,
          summary.activeDays,
          summary.longestStreak,
          summary.additions,
          summary.deletions,
          summary.netLines,
          summary.averageCommitsPerActiveDay,
          summary.workflowRuns,
          summary.workflowSuccess,
          summary.workflowFailure,
          summary.workflowCancelled,
          summary.workflowSkipped,
          summary.workflowOther,
          summary.busiestDay?.date ?? null,
          summary.busiestDay?.commits ?? null,
          summary.busiestMonth?.month ?? null,
          summary.busiestMonth?.commits ?? null,
          JSON.stringify(yearData.scope),
        );

        for (const project of yearData.projects) {
          insertProject.run(
            yearData.year,
            project.id,
            project.owner,
            project.name,
            project.displayName,
            project.visibility,
            project.isPrivate ? 1 : 0,
            project.isArchived ? 1 : 0,
            project.url,
            project.description,
            project.homepageUrl,
            project.repoPath,
            project.stars,
            project.forks,
            project.createdAt,
            project.updatedAt,
            project.pushedAt,
            project.metrics.commits,
            project.metrics.activeDays,
            project.metrics.longestStreak,
            project.metrics.additions,
            project.metrics.deletions,
            project.metrics.netLines,
            project.metrics.changedFiles,
            project.metrics.firstCommitAt,
            project.metrics.lastCommitAt,
            project.metrics.averageCommitsPerActiveDay,
            project.workflows.total,
            project.workflows.success,
            project.workflows.failure,
            project.workflows.cancelled,
            project.workflows.skipped,
            project.workflows.other,
            JSON.stringify(project.source),
          );

          for (const language of project.languages) {
            insertProjectLanguage.run(
              yearData.year,
              project.id,
              language.name,
              language.color,
              language.bytes,
              language.ratio,
              language.estimatedWeightedLines,
            );
          }

          for (const entry of project.chronology) {
            insertProjectChronology.run(
              yearData.year,
              project.id,
              entry.date,
              entry.commits,
              entry.additions,
              entry.deletions,
            );
          }

          for (const entry of project.months) {
            insertProjectMonth.run(
              yearData.year,
              project.id,
              entry.month,
              entry.commits,
              entry.additions,
              entry.deletions,
              entry.activeDays,
            );
          }

          for (const entry of project.weekdays) {
            insertProjectWeekday.run(
              yearData.year,
              project.id,
              entry.weekday,
              entry.commits,
              entry.additions,
              entry.deletions,
            );
          }

          for (const entry of project.hours) {
            insertProjectHour.run(
              yearData.year,
              project.id,
              entry.hour,
              entry.commits,
              entry.additions,
              entry.deletions,
            );
          }

          for (const entry of project.workflows.byEvent) {
            insertWorkflowEvent.run(yearData.year, project.id, entry.event, entry.count);
          }

          for (const entry of project.workflows.byMonth) {
            insertWorkflowMonth.run(yearData.year, project.id, entry.month, entry.count);
          }
        }
      }
      database.exec("COMMIT");
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  } finally {
    database.close();
  }
}

const isDirectRun = process.argv[1]
  ? path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;

if (isDirectRun) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

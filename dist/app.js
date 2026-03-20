const appData = window.__GITHUB_ACTIVITY__;
const dashboardName = appData.dashboardName || "dashboard";
const years = appData.years
  .slice()
  .sort((left, right) => right.year - left.year);

const pageMode = document.body.dataset.page;
const pageYear = Number(document.body.dataset.year || 0);
let currentVisibility = "all";

initializeTheme();

if (pageMode === "compare") {
  renderCombinedPage();
} else {
  renderYearPage(pageYear);
}

function renderCombinedPage() {
  const combinedData = buildCombinedDataset(years);
  setDetailModeLabels("combined", combinedData);
  renderVisibilityFilter(combinedData);
  renderYearView(combinedData, currentVisibility);
}

function renderYearPage(year) {
  const yearData = years.find((entry) => entry.year === year);
  if (!yearData) {
    setHero({
      eyebrow: `${dashboardName} · Year Missing`,
      title: "Год не найден в выгруженных данных.",
      copy: "Проверь, что нужный год присутствует в data/*.json и затем пересобери dashboard.",
      meta: [],
    });
    return;
  }

  setDetailModeLabels("year", yearData);
  renderVisibilityFilter(yearData);
  renderYearView(yearData, currentVisibility);
}

function renderVisibilityFilter(yearData) {
  const root = document.getElementById("visibility-filter");
  const options = [
    { value: "all", label: "All Projects" },
    { value: "public", label: "Public Only" },
    { value: "private", label: "Private Only" },
  ];

  root.innerHTML = "";
  for (const option of options) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `filter-button ${option.value === currentVisibility ? "active" : ""}`;
    button.textContent = option.label;
    button.setAttribute("aria-pressed", option.value === currentVisibility ? "true" : "false");
    button.addEventListener("click", () => {
      currentVisibility = option.value;
      document.querySelectorAll(".filter-button").forEach((node) => {
        node.classList.remove("active");
        node.setAttribute("aria-pressed", "false");
      });
      button.classList.add("active");
      button.setAttribute("aria-pressed", "true");
      renderYearView(yearData, currentVisibility);
    });
    root.appendChild(button);
  }
}

function renderYearView(yearData, visibility) {
  const filteredProjects = filterProjects(yearData.projects, visibility);
  const aggregate = aggregateYearView(yearData, filteredProjects);
  const rangeText = yearData.yearRange.isCombined
    ? `${yearData.yearRange.combinedYears.join(" + ")} как единый период`
    : yearData.yearRange.isCurrentYear
    ? "данные с начала года по текущую дату"
    : "полный календарный год";
  const pageCopy = yearData.yearRange.isCombined
    ? `${yearData.viewer.name} · ${formatNumber(aggregate.summary.projectsWorkedOn)} проектов, ${formatNumber(aggregate.summary.commits)} коммитов и ${formatNumber(aggregate.summary.activeDays)} активных дней. Ниже объединённый срез за период ${yearData.yearRange.combinedYears.join("-")}.`
    : `${yearData.viewer.name} · ${formatNumber(aggregate.summary.projectsWorkedOn)} проектов, ${formatNumber(aggregate.summary.commits)} коммитов и ${formatNumber(aggregate.summary.activeDays)} активных дней. ${rangeText}. Ниже компактный инфографичный срез по ритму, delivery и проектам.`;

  setHero({
    eyebrow: `${dashboardName} · ${visibilityLabel(visibility)}`,
    title: `${yearData.year} / GitHub Activity`,
    copy: pageCopy,
    meta: [
      `Period: ${formatDate(yearData.yearRange.since)} - ${formatDate(yearData.yearRange.until)}`,
      `Timezone: ${yearData.timezone}`,
      `Filter: ${visibilityLabel(visibility)}`,
      `Generated: ${formatDateTime(yearData.generatedAt)}`,
    ],
  });

  document.getElementById("kpis").innerHTML = [
    {
      label: "Projects",
      value: aggregate.summary.projectsWorkedOn,
      detail: `${aggregate.summary.publicProjects} public / ${aggregate.summary.privateProjects} private`,
    },
    {
      label: "Commits",
      value: aggregate.summary.commits,
      detail: `${aggregate.summary.averageCommitsPerActiveDay} per active day`,
    },
    {
      label: "Active Days",
      value: aggregate.summary.activeDays,
      detail: `longest streak ${aggregate.summary.longestStreak} days`,
    },
    {
      label: "Line Churn",
      value: formatSigned(aggregate.summary.netLines),
      detail: `+${formatNumber(aggregate.summary.additions)} / -${formatNumber(aggregate.summary.deletions)}`,
    },
    {
      label: "Workflow Runs",
      value: aggregate.workflows.total,
      detail: `${aggregate.workflows.success} success / ${aggregate.workflows.failure} failure`,
    },
    {
      label: "Busiest Day",
      value: aggregate.summary.busiestDay ? aggregate.summary.busiestDay.date : "-",
      detail: aggregate.summary.busiestDay ? `${formatNumber(aggregate.summary.busiestDay.commits)} commits` : "No data",
    },
    {
      label: "Busiest Month",
      value: aggregate.summary.busiestMonth ? aggregate.summary.busiestMonth.month : "-",
      detail: aggregate.summary.busiestMonth ? `${formatNumber(aggregate.summary.busiestMonth.commits)} commits` : "No data",
    },
    {
      label: "Organizations",
      value: aggregate.byOwner.length,
      detail: aggregate.byOwner.slice(0, 3).map((entry) => entry.owner).join(", ") || "No data",
    },
  ]
    .map(renderKpiCard)
    .join("");

  document.getElementById("org-bars").innerHTML = buildBars(
    aggregate.byOwner,
    "owner",
    "commits",
    "commits",
  );
  document.getElementById("top-public").innerHTML = buildTopProjects(
    filteredProjects.filter((project) => !project.isPrivate),
  );
  document.getElementById("top-private").innerHTML = buildTopProjects(
    filteredProjects.filter((project) => project.isPrivate),
  );
  document.getElementById("chronology-chart").innerHTML = buildChronologyChart(aggregate.chronology, aggregate.months);
  document.getElementById("months-chart").innerHTML = buildMonthChart(aggregate.months);
  document.getElementById("weekday-bars").innerHTML = buildBars(aggregate.weekdays, "weekday", "commits", "commits");
  document.getElementById("hours-grid").innerHTML = buildHourChart(aggregate.hours);
  document.getElementById("language-bars").innerHTML = buildBars(aggregate.languages.slice(0, 8), "name", "weightedLines", "weighted lines");
  document.getElementById("workflow-bars").innerHTML = buildWorkflowSection(aggregate.workflows);
  document.getElementById("projects-body").innerHTML = buildProjects(filteredProjects);
  document.getElementById("footer-note").textContent =
    yearData.yearRange.isCombined
      ? "Методика: период 2025-2026 собран как единый срез. Репозитории склеены по project id, активность и line churn агрегированы, private repo замаскированы как project N."
      : "Методика: репозитории обнаруживаются по owner и локальным clone remote, затем в итог попадают только те, где действительно нашлись твои коммиты за выбранный год. Private repo замаскированы как project N.";
}

function setDetailModeLabels(mode, data) {
  const markers = document.querySelectorAll("#detail-root .section-marker");
  if (!markers.length) {
    return;
  }

  const firstHeading = markers[0].querySelector("h2");
  const firstCopy = markers[0].querySelector("p");
  if (mode === "combined") {
    if (firstHeading) {
      firstHeading.textContent = "Period Summary";
    }
    if (firstCopy) {
      firstCopy.textContent = `Общий срез за ${data.yearRange.combinedYears.join("-")}.`;
    }
  } else {
    if (firstHeading) {
      firstHeading.textContent = "Summary";
    }
    if (firstCopy) {
      firstCopy.textContent = "Объём года, фильтры и ключевые проекты.";
    }
  }
}

function setHero({ eyebrow, title, copy, meta }) {
  document.getElementById("hero-eyebrow").textContent = eyebrow;
  document.getElementById("page-title").textContent = title;
  document.getElementById("page-copy").textContent = copy;
  document.getElementById("meta-line").innerHTML = meta.map((item) => `<span>${escapeHtml(item)}</span>`).join("");
}

function initializeTheme() {
  const storedTheme = localStorage.getItem("deksden-dashboard-theme");
  const preferredDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  const theme = storedTheme === "light" || storedTheme === "dark"
    ? storedTheme
    : preferredDark
      ? "dark"
      : "light";

  applyTheme(theme);

  const toggle = document.getElementById("theme-toggle");
  const label = document.getElementById("theme-toggle-label");
  if (!toggle || !label) {
    return;
  }

  toggle.addEventListener("click", () => {
    const nextTheme = document.body.dataset.theme === "dark" ? "light" : "dark";
    applyTheme(nextTheme);
    localStorage.setItem("deksden-dashboard-theme", nextTheme);
  });
}

function applyTheme(theme) {
  document.body.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
  const toggle = document.getElementById("theme-toggle");
  const label = document.getElementById("theme-toggle-label");
  const metaTheme = document.querySelector('meta[name="theme-color"]');
  if (toggle) {
    toggle.classList.toggle("active", theme === "dark");
    toggle.setAttribute("aria-pressed", theme === "dark" ? "true" : "false");
    toggle.setAttribute("aria-label", theme === "dark" ? "Switch To Light Theme" : "Switch To Dark Theme");
  }
  if (label) {
    label.textContent = theme === "dark" ? "Dark Theme" : "Light Theme";
  }
  if (metaTheme) {
    metaTheme.setAttribute("content", theme === "dark" ? "#0b1020" : "#efe8dc");
  }
}

function filterProjects(projects, visibility) {
  if (visibility === "public") {
    return projects.filter((project) => !project.isPrivate);
  }
  if (visibility === "private") {
    return projects.filter((project) => project.isPrivate);
  }
  return projects.slice();
}

function buildCombinedDataset(yearEntries) {
  const orderedYears = yearEntries
    .slice()
    .sort((left, right) => left.year - right.year);
  const combinedYears = orderedYears.map((entry) => entry.year);
  const projectMap = new Map();

  for (const yearData of orderedYears) {
    for (const project of yearData.projects) {
      const existing = projectMap.get(project.id) ?? createCombinedProject(project);
      mergeCombinedProject(existing, project);
      projectMap.set(project.id, existing);
    }
  }

  const projects = [...projectMap.values()]
    .map(finalizeCombinedProject)
    .sort((left, right) => right.metrics.commits - left.metrics.commits);

  return {
    year: `${combinedYears[0]}-${combinedYears.at(-1)}`,
    generatedAt: appData.generatedAt,
    timezone: orderedYears[0]?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone,
    viewer: orderedYears[0]?.viewer ?? { name: dashboardName },
    yearRange: {
      since: orderedYears[0]?.yearRange?.since,
      until: orderedYears.at(-1)?.yearRange?.until,
      isCurrentYear: false,
      isCombined: true,
      combinedYears,
    },
    projects,
  };
}

function createCombinedProject(project) {
  return {
    ...project,
    metrics: {
      commits: 0,
      activeDays: 0,
      longestStreak: 0,
      additions: 0,
      deletions: 0,
      netLines: 0,
      changedFiles: 0,
      firstCommitAt: null,
      lastCommitAt: null,
      averageCommitsPerActiveDay: 0,
    },
    chronology: [],
    months: [],
    weekdays: [],
    hours: [],
    languages: [],
    workflows: {
      total: 0,
      success: 0,
      failure: 0,
      cancelled: 0,
      skipped: 0,
      other: 0,
      byEvent: [],
      byMonth: [],
    },
  };
}

function mergeCombinedProject(target, project) {
  target.metrics.commits += project.metrics.commits;
  target.metrics.additions += project.metrics.additions;
  target.metrics.deletions += project.metrics.deletions;
  target.metrics.netLines += project.metrics.netLines;
  target.metrics.changedFiles += project.metrics.changedFiles;
  target.metrics.firstCommitAt = minIso(target.metrics.firstCommitAt, project.metrics.firstCommitAt);
  target.metrics.lastCommitAt = maxIso(target.metrics.lastCommitAt, project.metrics.lastCommitAt);

  target.workflows.total += project.workflows.total;
  target.workflows.success += project.workflows.success;
  target.workflows.failure += project.workflows.failure;
  target.workflows.cancelled += project.workflows.cancelled;
  target.workflows.skipped += project.workflows.skipped;
  target.workflows.other += project.workflows.other;

  target.chronology = mergeBuckets(target.chronology, project.chronology, "date");
  target.months = mergeBuckets(target.months, project.months, "month");
  target.weekdays = mergeBuckets(target.weekdays, project.weekdays, "weekday");
  target.hours = mergeBuckets(target.hours, project.hours, "hour");
  target.languages = mergeLanguages(target.languages, project.languages);
  target.workflows.byEvent = mergeCountBuckets(target.workflows.byEvent, project.workflows.byEvent, "event");
  target.workflows.byMonth = mergeCountBuckets(target.workflows.byMonth, project.workflows.byMonth, "month");
}

function finalizeCombinedProject(project) {
  const chronology = project.chronology.slice().sort((left, right) => String(left.date).localeCompare(String(right.date)));
  const activeDays = chronology.length;
  const months = project.months.slice().sort((left, right) => String(left.month).localeCompare(String(right.month)));
  const weekdays = project.weekdays.slice().sort((left, right) => weekdayIndex(left.weekday) - weekdayIndex(right.weekday));
  const hours = project.hours.slice().sort((left, right) => left.hour - right.hour);
  const languages = project.languages.slice().sort((left, right) => right.estimatedWeightedLines - left.estimatedWeightedLines);
  const byEvent = project.workflows.byEvent.slice().sort((left, right) => right.count - left.count);
  const byMonth = project.workflows.byMonth.slice().sort((left, right) => String(left.month).localeCompare(String(right.month)));

  return {
    ...project,
    chronology,
    months,
    weekdays,
    hours,
    languages,
    metrics: {
      ...project.metrics,
      activeDays,
      longestStreak: calculateLongestStreak(chronology.map((entry) => entry.date)),
      averageCommitsPerActiveDay: activeDays > 0 ? round(project.metrics.commits / activeDays, 2) : 0,
    },
    workflows: {
      ...project.workflows,
      byEvent,
      byMonth,
    },
  };
}

function mergeBuckets(leftItems, rightItems, key) {
  const map = new Map(leftItems.map((entry) => [entry[key], { ...entry }]));
  for (const entry of rightItems) {
    const existing = map.get(entry[key]) ?? { ...entry };
    if (map.has(entry[key])) {
      existing.commits += entry.commits ?? 0;
      existing.additions += entry.additions ?? 0;
      existing.deletions += entry.deletions ?? 0;
      if ("activeDays" in existing || "activeDays" in entry) {
        existing.activeDays = (existing.activeDays ?? 0) + (entry.activeDays ?? 0);
      }
      if ("workflowRuns" in existing || "workflowRuns" in entry) {
        existing.workflowRuns = (existing.workflowRuns ?? 0) + (entry.workflowRuns ?? 0);
      }
    }
    map.set(entry[key], existing);
  }
  return [...map.values()];
}

function mergeCountBuckets(leftItems, rightItems, key) {
  const map = new Map(leftItems.map((entry) => [entry[key], { ...entry }]));
  for (const entry of rightItems) {
    const existing = map.get(entry[key]) ?? { ...entry };
    if (map.has(entry[key])) {
      existing.count += entry.count ?? 0;
    }
    map.set(entry[key], existing);
  }
  return [...map.values()];
}

function mergeLanguages(leftItems, rightItems) {
  const map = new Map(leftItems.map((entry) => [entry.name, { ...entry }]));
  for (const entry of rightItems) {
    const existing = map.get(entry.name) ?? { ...entry };
    if (map.has(entry.name)) {
      existing.bytes += entry.bytes ?? 0;
      existing.estimatedWeightedLines += entry.estimatedWeightedLines ?? 0;
    }
    map.set(entry.name, existing);
  }
  return [...map.values()];
}

function minIso(left, right) {
  if (!left) return right;
  if (!right) return left;
  return left < right ? left : right;
}

function maxIso(left, right) {
  if (!left) return right;
  if (!right) return left;
  return left > right ? left : right;
}

function weekdayIndex(value) {
  return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].indexOf(value);
}

function aggregateYearView(yearData, projects) {
  const chronologyMap = new Map();
  const monthMap = new Map();
  const weekdayMap = new Map(
    ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((weekday) => [
      weekday,
      { weekday, commits: 0, additions: 0, deletions: 0 },
    ]),
  );
  const hourMap = new Map(
    Array.from({ length: 24 }, (_, hour) => [
      hour,
      { hour, commits: 0, additions: 0, deletions: 0 },
    ]),
  );
  const languageMap = new Map();
  const workflowByEvent = new Map();
  const workflowByMonth = new Map();
  const ownerMap = new Map();
  const activeDaysSet = new Set();

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
    workflowTotal += project.workflows.total;
    workflowSuccess += project.workflows.success;
    workflowFailure += project.workflows.failure;
    workflowCancelled += project.workflows.cancelled;
    workflowSkipped += project.workflows.skipped;
    workflowOther += project.workflows.other;

    const owner = project.ownerLabel || project.owner;
    const ownerEntry = ownerMap.get(owner) ?? {
      owner,
      commits: 0,
      projects: 0,
      activeDays: 0,
      netLines: 0,
    };
    ownerEntry.commits += project.metrics.commits;
    ownerEntry.projects += 1;
    ownerEntry.activeDays += project.metrics.activeDays;
    ownerEntry.netLines += project.metrics.netLines;
    ownerMap.set(owner, ownerEntry);

    for (const entry of project.chronology) {
      const bucket = chronologyMap.get(entry.date) ?? {
        date: entry.date,
        commits: 0,
        additions: 0,
        deletions: 0,
      };
      bucket.commits += entry.commits;
      bucket.additions += entry.additions;
      bucket.deletions += entry.deletions;
      chronologyMap.set(entry.date, bucket);
      activeDaysSet.add(entry.date);

      const monthKey = entry.date.slice(0, 7);
      const monthBucket = monthMap.get(monthKey) ?? {
        month: monthKey,
        commits: 0,
        additions: 0,
        deletions: 0,
        activeDaysSet: new Set(),
      };
      monthBucket.activeDaysSet.add(entry.date);
      monthMap.set(monthKey, monthBucket);
    }

    for (const entry of project.months) {
      const bucket = monthMap.get(entry.month) ?? {
        month: entry.month,
        commits: 0,
        additions: 0,
        deletions: 0,
        activeDaysSet: new Set(),
      };
      bucket.commits += entry.commits;
      bucket.additions += entry.additions;
      bucket.deletions += entry.deletions;
      monthMap.set(entry.month, bucket);
    }

    for (const entry of project.weekdays) {
      const bucket = weekdayMap.get(entry.weekday);
      bucket.commits += entry.commits;
      bucket.additions += entry.additions;
      bucket.deletions += entry.deletions;
    }

    for (const entry of project.hours) {
      const bucket = hourMap.get(entry.hour);
      bucket.commits += entry.commits;
      bucket.additions += entry.additions;
      bucket.deletions += entry.deletions;
    }

    for (const entry of project.languages) {
      const bucket = languageMap.get(entry.name) ?? {
        name: entry.name,
        color: entry.color,
        weightedLines: 0,
        repoCount: 0,
      };
      bucket.weightedLines += entry.estimatedWeightedLines;
      bucket.repoCount += 1;
      languageMap.set(entry.name, bucket);
    }

    for (const entry of project.workflows.byEvent) {
      workflowByEvent.set(entry.event, (workflowByEvent.get(entry.event) ?? 0) + entry.count);
    }

    for (const entry of project.workflows.byMonth) {
      workflowByMonth.set(entry.month, (workflowByMonth.get(entry.month) ?? 0) + entry.count);
    }
  }

  const chronology = [...chronologyMap.values()].sort((left, right) => left.date.localeCompare(right.date));
  const months = [...monthMap.values()]
    .map((entry) => ({
      month: entry.month,
      commits: entry.commits,
      additions: entry.additions,
      deletions: entry.deletions,
      activeDays: entry.activeDaysSet.size,
      workflowRuns: workflowByMonth.get(entry.month) ?? 0,
    }))
    .sort((left, right) => left.month.localeCompare(right.month));

  const busiestDay = chronology.slice().sort((left, right) => right.commits - left.commits)[0] ?? null;
  const busiestMonth = months.slice().sort((left, right) => right.commits - left.commits)[0] ?? null;
  const activeDays = [...activeDaysSet].sort();

  return {
    filteredProjects: projects,
    summary: {
      projectsWorkedOn: projects.length,
      publicProjects: projects.filter((project) => !project.isPrivate).length,
      privateProjects: projects.filter((project) => project.isPrivate).length,
      commits,
      additions,
      deletions,
      netLines: additions - deletions,
      activeDays: activeDays.length,
      longestStreak: calculateLongestStreak(activeDays),
      averageCommitsPerActiveDay: activeDays.length > 0 ? round(commits / activeDays.length, 2) : 0,
      workflowRuns: workflowTotal,
      busiestDay,
      busiestMonth,
    },
    chronology,
    months,
    weekdays: [...weekdayMap.values()],
    hours: [...hourMap.values()],
    languages: [...languageMap.values()].sort((left, right) => right.weightedLines - left.weightedLines),
    workflows: {
      total: workflowTotal,
      success: workflowSuccess,
      failure: workflowFailure,
      cancelled: workflowCancelled,
      skipped: workflowSkipped,
      other: workflowOther,
      byEvent: [...workflowByEvent.entries()]
        .map(([event, count]) => ({ event, count }))
        .sort((left, right) => right.count - left.count),
      byMonth: [...workflowByMonth.entries()]
        .map(([month, count]) => ({ month, count }))
        .sort((left, right) => left.month.localeCompare(right.month)),
    },
    byOwner: [...ownerMap.values()].sort((left, right) => right.commits - left.commits),
  };
}

function buildDeltaGrid(latest, previous) {
  const metrics = [
    { label: "Commits", latest: latest.summary.commits, previous: previous.summary.commits },
    { label: "Projects", latest: latest.summary.projectsWorkedOn, previous: previous.summary.projectsWorkedOn },
    { label: "Active Days", latest: latest.summary.activeDays, previous: previous.summary.activeDays },
    { label: "Net Lines", latest: latest.summary.netLines, previous: previous.summary.netLines },
    { label: "Workflow Runs", latest: latest.summary.workflowRuns, previous: previous.summary.workflowRuns },
    { label: "Private Projects", latest: latest.summary.privateProjects, previous: previous.summary.privateProjects },
  ];

  return metrics
    .map((metric) => {
      const delta = metric.latest - metric.previous;
      const className = delta >= 0 ? "up" : "down";
      const prefix = delta >= 0 ? "+" : "";
      return `
        <div class="delta panel pad">
          <div class="muted">${metric.label}</div>
          <strong>${formatNumber(metric.latest)}</strong>
          <div class="${className}">${prefix}${formatNumber(delta)} vs ${previous.year}</div>
        </div>
      `;
    })
    .join("");
}

function renderYearCard(yearData) {
  const ownerSummary = summarizeOwners(yearData.projects)
    .slice(0, 2)
    .map((entry) => `${entry.owner}: ${formatNumber(entry.commits)}`)
    .join(" · ");

  return `
    <article class="panel pad">
      <div class="section-head">
        <div><h2>${yearData.year}</h2></div>
        <a class="nav-link" href="./year-${yearData.year}.html">Open Year</a>
      </div>
      <div class="stack">
        <div class="bar-row">
          <div class="bar-label">Commits</div>
          <div class="bar-track"><div class="bar-fill" style="width:100%"></div></div>
          <div class="bar-value">${formatNumber(yearData.summary.commits)}</div>
        </div>
        <div class="muted">${formatNumber(yearData.summary.projectsWorkedOn)} projects · ${formatNumber(yearData.summary.activeDays)} active days</div>
        <div class="muted">${yearData.summary.publicProjects} public / ${yearData.summary.privateProjects} private</div>
        <div class="muted">Net lines: ${formatSigned(yearData.summary.netLines)}</div>
        <div class="muted">Owners: ${ownerSummary || "No data"}</div>
      </div>
    </article>
  `;
}

function buildCompareOwnerBars(yearsInput) {
  const rows = yearsInput.flatMap((yearData) =>
    summarizeOwners(yearData.projects).map((entry) => ({
      owner: `${yearData.year} · ${entry.owner}`,
      commits: entry.commits,
    })),
  );
  return buildBars(rows, "owner", "commits", "commits");
}

function buildVisibilityCompareTable(yearsInput) {
  return `
    <table class="compare-table">
      <thead>
        <tr>
          <th>Year</th>
          <th>Public Projects</th>
          <th>Private Projects</th>
          <th>Public Commits</th>
          <th>Private Commits</th>
        </tr>
      </thead>
      <tbody>
        ${yearsInput
          .map((yearData) => {
            const publicProjects = yearData.projects.filter((project) => !project.isPrivate);
            const privateProjects = yearData.projects.filter((project) => project.isPrivate);
            return `
              <tr>
                <td>${yearData.year}</td>
                <td>${formatNumber(publicProjects.length)}</td>
                <td>${formatNumber(privateProjects.length)}</td>
                <td>${formatNumber(sum(publicProjects.map((project) => project.metrics.commits)))}</td>
                <td>${formatNumber(sum(privateProjects.map((project) => project.metrics.commits)))}</td>
              </tr>
            `;
          })
          .join("")}
      </tbody>
    </table>
  `;
}

function buildCompareTopLists(yearsInput, isPrivate) {
  return yearsInput
    .map((yearData) => {
      const projects = yearData.projects
        .filter((project) => project.isPrivate === isPrivate)
        .slice(0, 5);
      return `
        <div class="panel pad">
          <div class="section-head">
            <div><h2>${yearData.year}</h2></div>
            <div class="muted">${projects.length} projects shown</div>
          </div>
          ${buildTopProjects(projects)}
        </div>
      `;
    })
    .join("");
}

function buildTopProjects(projects) {
  if (!projects.length) {
    return '<div class="muted">No projects in this scope.</div>';
  }

  const top = projects
    .slice()
    .sort((left, right) => right.metrics.commits - left.metrics.commits)
    .slice(0, 5)
    .map((project) => ({
      name: project.displayName,
      commits: project.metrics.commits,
      owner: project.ownerLabel || project.owner,
    }));

  const maxValue = Math.max(...top.map((entry) => entry.commits), 1);
  return top
    .map(
      (entry) => `
        <div class="bar-row">
          <div class="bar-label">${escapeHtml(entry.name)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${(entry.commits / maxValue) * 100}%"></div></div>
          <div class="bar-value">${formatNumber(entry.commits)} · ${escapeHtml(entry.owner)}</div>
        </div>
      `,
    )
    .join("");
}

function summarizeOwners(projects) {
  const ownerMap = new Map();
  for (const project of projects) {
    const owner = project.ownerLabel || project.owner;
    const entry = ownerMap.get(owner) ?? { owner, projects: 0, commits: 0 };
    entry.projects += 1;
    entry.commits += project.metrics.commits;
    ownerMap.set(owner, entry);
  }
  return [...ownerMap.values()].sort((left, right) => right.commits - left.commits);
}

function renderKpiCard(card) {
  return `
    <article class="panel pad kpi">
      <div class="label">${card.label}</div>
      <div class="value">${card.value}</div>
      <div class="detail">${card.detail}</div>
    </article>
  `;
}

function buildChronologyChart(chronology, months) {
  if (!chronology.length) {
    return "<p>Нет данных для хронологии.</p>";
  }

  const width = 760;
  const height = 236;
  const padding = { top: 16, right: 16, bottom: 34, left: 16 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxCommits = Math.max(...chronology.map((entry) => entry.commits), 1);
  const maxLines = Math.max(...chronology.map((entry) => entry.additions + entry.deletions), 1);
  const step = chronology.length > 1 ? chartWidth / (chronology.length - 1) : 0;

  const commitPath = chronology
    .map((entry, index) => {
      const x = padding.left + step * index;
      const y = padding.top + chartHeight - (entry.commits / maxCommits) * chartHeight;
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");

  const areaPath = `${commitPath} L ${padding.left + step * (chronology.length - 1)} ${height - padding.bottom} L ${padding.left} ${height - padding.bottom} Z`;

  const linePath = chronology
    .map((entry, index) => {
      const x = padding.left + step * index;
      const y = padding.top + chartHeight - ((entry.additions + entry.deletions) / maxLines) * chartHeight;
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");

  const circles = chronology
    .map((entry, index) => {
      const x = padding.left + step * index;
      const y = padding.top + chartHeight - (entry.commits / maxCommits) * chartHeight;
      return `<circle cx="${x}" cy="${y}" r="3.5" fill="var(--accent)" opacity="0.82"><title>${entry.date}: ${entry.commits} commits</title></circle>`;
    })
    .join("");

  const gridLines = Array.from({ length: 4 }, (_, index) => {
    const ratio = index / 3;
    const y = padding.top + chartHeight * ratio;
    const label = formatNumber(Math.round(maxCommits * (1 - ratio)));
    return `
      <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="chart-grid-line"></line>
      <text x="${width - padding.right}" y="${y - 6}" class="chart-axis-label" text-anchor="end">${label}</text>
    `;
  }).join("");

  const activeMonths = months.filter((entry) => entry.commits > 0);
  const monthSegments = activeMonths
    .map((entry, index) => {
      const indexes = chronology
        .map((bucket, bucketIndex) => (bucket.date.startsWith(entry.month) ? bucketIndex : -1))
        .filter((value) => value >= 0);
      if (!indexes.length) {
        return null;
      }

      const startX = padding.left + step * indexes[0];
      const endX = padding.left + step * indexes[indexes.length - 1];
      const midX = startX + (endX - startX) / 2;
      return {
        month: entry.month,
        commits: entry.commits,
        startX,
        endX,
        midX,
        shaded: index % 2 === 0,
      };
    })
    .filter(Boolean);

  const monthBands = monthSegments
    .map(
      (entry) => `
        <rect x="${entry.startX}" y="${padding.top}" width="${Math.max(entry.endX - entry.startX, 10)}" height="${chartHeight}" class="chart-month-band${entry.shaded ? " shaded" : ""}"></rect>
        <line x1="${entry.startX}" y1="${padding.top}" x2="${entry.startX}" y2="${height - padding.bottom}" class="chart-month-line"></line>
      `,
    )
    .join("");

  const monthLabels = monthSegments
    .map(
      (entry) => `
        <text x="${entry.midX}" y="${height - 10}" class="chart-axis-label" text-anchor="middle">${escapeHtml(formatMonthAxis(entry.month))}</text>
      `,
    )
    .join("");

  return `
    <div class="chart-shell">
      <svg viewBox="0 0 ${width} ${height}" width="100%" height="236" class="chart-svg" aria-label="Chronology chart">
        <defs>
          <linearGradient id="commitFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="var(--accent)" stop-opacity="0.34" />
            <stop offset="100%" stop-color="var(--accent)" stop-opacity="0.03" />
          </linearGradient>
        </defs>
        ${monthBands}
        ${gridLines}
        <path d="${areaPath}" fill="url(#commitFill)"></path>
        <path d="${linePath}" fill="none" stroke="var(--accent-2)" stroke-width="3" stroke-linecap="round" opacity="0.95"></path>
        <path d="${commitPath}" fill="none" stroke="var(--accent)" stroke-width="4" stroke-linecap="round"></path>
        ${circles}
        ${monthLabels}
      </svg>
      <div class="chart-legend">
        <span class="legend-item"><span class="legend-swatch"></span>Commits</span>
        <span class="legend-item"><span class="legend-swatch cool"></span>Churn</span>
        <span class="legend-item"><span class="legend-swatch muted"></span>Months</span>
      </div>
    </div>
  `;
}

function buildMonthChart(months) {
  const activeMonths = months.filter((entry) => entry.commits > 0 || entry.workflowRuns > 0);
  if (!activeMonths.length) {
    return "<p>Нет данных по месяцам.</p>";
  }

  const maxCommits = Math.max(...activeMonths.map((entry) => entry.commits), 1);
  const maxWorkflows = Math.max(...activeMonths.map((entry) => entry.workflowRuns), 1);
  return `
    <div class="month-chart">
      ${activeMonths
        .map((entry) => {
          const percent = Math.max((entry.commits / maxCommits) * 100, 8);
          const workflowOffset = entry.workflowRuns
            ? Math.max((entry.workflowRuns / maxWorkflows) * 100, 10)
            : null;
          return `
            <div class="month-column">
              <div class="month-value">${formatNumber(entry.commits)}</div>
              <div class="month-bar-shell">
                <div class="month-bar" style="height:${percent}%"></div>
                ${workflowOffset ? `<div class="month-marker" style="bottom:${workflowOffset}%"><span></span></div>` : ""}
              </div>
              <div class="month-label">${formatMonthShort(entry.month)}</div>
              <div class="month-meta">${entry.activeDays}d${entry.workflowRuns ? ` · ${formatNumber(entry.workflowRuns)}r` : ""}</div>
            </div>
          `;
        })
        .join("")}
    </div>
    <div class="chart-legend">
      <span class="legend-item"><span class="legend-swatch"></span>Commits</span>
      <span class="legend-item"><span class="legend-swatch gold"></span>Runs</span>
    </div>
  `;
}

function buildBars(items, labelKey, valueKey, suffix) {
  if (!items.length) {
    return "<p>Нет данных.</p>";
  }

  const maxValue = Math.max(...items.map((entry) => entry[valueKey]), 1);
  return items
    .map(
      (entry) => `
        <div class="bar-row">
          <div class="bar-label">${escapeHtml(String(entry[labelKey]))}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${(entry[valueKey] / maxValue) * 100}%"></div></div>
          <div class="bar-value">${formatNumber(entry[valueKey])} ${suffix}</div>
        </div>
      `,
    )
    .join("");
}

function buildHourChart(hours) {
  if (!hours.length) {
    return "<p>Нет данных.</p>";
  }

  const maxCommits = Math.max(...hours.map((entry) => entry.commits), 1);
  const maxLines = Math.max(...hours.map((entry) => entry.additions + entry.deletions), 1);
  const peakThreshold = [...hours]
    .sort((left, right) => right.commits - left.commits)
    .slice(0, 4)
    .at(-1)?.commits ?? 0;

  return `
    <div class="hour-chart">
      ${hours
        .map((entry) => {
          const commitHeight = entry.commits > 0 ? Math.max((entry.commits / maxCommits) * 100, 8) : 4;
          const churnHeight = entry.additions + entry.deletions > 0
            ? Math.max(((entry.additions + entry.deletions) / maxLines) * 100, 10)
            : 4;
          const isPeak = entry.commits >= peakThreshold && entry.commits > 0;
          const tickLabel = entry.hour % 3 === 0 || entry.hour === 23 ? String(entry.hour).padStart(2, "0") : "";
          return `
            <div class="hour-column${isPeak ? " peak" : ""}">
              <div class="hour-bar-shell" title="${String(entry.hour).padStart(2, "0")}:00 · ${formatNumber(entry.commits)} commits · ${formatNumber(entry.additions + entry.deletions)} lines churn">
                <div class="hour-bar-churn" style="height:${churnHeight}%"></div>
                <div class="hour-bar-commit" style="height:${commitHeight}%"></div>
              </div>
              <div class="hour-count">${formatNumber(entry.commits)}</div>
              <div class="hour-axis">${tickLabel}</div>
            </div>
          `;
        })
        .join("")}
    </div>
    <div class="chart-legend">
      <span class="legend-item"><span class="legend-swatch"></span>Commits</span>
      <span class="legend-item"><span class="legend-swatch cool"></span>Churn</span>
      <span class="legend-item"><span class="legend-swatch gold"></span>Peaks</span>
    </div>
  `;
}

function buildHourGrid(hours) {
  return buildHourChart(hours);
}

function buildWorkflowSection(workflows) {
  if (!workflows.total) {
    return '<p class="muted">В доступных репозиториях за этот период workflow runs по actor не зафиксированы.</p>';
  }

  return [
    buildBars(workflows.byEvent.slice(0, 6), "event", "count", "runs"),
    `<div class="footer-note">success: ${formatNumber(workflows.success)} · failure: ${formatNumber(workflows.failure)} · cancelled: ${formatNumber(workflows.cancelled)} · skipped: ${formatNumber(workflows.skipped)}</div>`,
  ].join("");
}

function buildProjects(projects) {
  if (!projects.length) {
    return '<tr><td colspan="8">Нет данных по проектам.</td></tr>';
  }

  return projects
    .map((project) => {
      const languageSummary = project.languages.slice(0, 3).map((entry) => entry.name).join(", ") || "-";
      const title = project.safeUrl
        ? `<a href="${project.safeUrl}" target="_blank" rel="noreferrer noopener">${escapeHtml(project.displayName)}</a>`
        : escapeHtml(project.displayName);
      const subtitle = project.safeDescription || project.safeOwner;

      return `
        <tr>
          <td>
            <div class="project-name">
              ${title}
              <span class="badge ${project.isPrivate ? "private" : ""}">${project.visibility}</span>
            </div>
            <div class="project-subtitle">${escapeHtml(subtitle || "Без описания")}</div>
          </td>
          <td>${escapeHtml(project.ownerLabel || project.owner)}</td>
          <td>${formatNumber(project.metrics.commits)}</td>
          <td>${formatNumber(project.metrics.activeDays)}</td>
          <td>${formatNumber(project.metrics.longestStreak)} days</td>
          <td>+${formatNumber(project.metrics.additions)} / -${formatNumber(project.metrics.deletions)}</td>
          <td>${formatNumber(project.workflows.total)}</td>
          <td>${escapeHtml(languageSummary)}</td>
        </tr>
      `;
    })
    .join("");
}

function visibilityLabel(value) {
  if (value === "public") return "public only";
  if (value === "private") return "private only";
  return "all projects";
}

function calculateLongestStreak(sortedDays) {
  if (!sortedDays.length) {
    return 0;
  }

  let longest = 1;
  let current = 1;
  for (let index = 1; index < sortedDays.length; index += 1) {
    const previous = new Date(`${sortedDays[index - 1]}T00:00:00Z`);
    const next = new Date(`${sortedDays[index]}T00:00:00Z`);
    const diff = Math.round((next - previous) / 86400000);
    if (diff === 1) {
      current += 1;
      longest = Math.max(longest, current);
    } else {
      current = 1;
    }
  }
  return longest;
}

function sum(values) {
  return values.reduce((total, value) => total + value, 0);
}

function round(value, digits = 0) {
  return Number(value.toFixed(digits));
}

function formatNumber(value) {
  return new Intl.NumberFormat("ru-RU").format(value ?? 0);
}

function formatSigned(value) {
  if (!value) {
    return "0";
  }
  return `${value > 0 ? "+" : ""}${formatNumber(value)}`;
}

function formatDate(isoString) {
  return new Intl.DateTimeFormat("ru-RU", {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(new Date(isoString));
}

function formatDateTime(isoString) {
  return new Intl.DateTimeFormat("ru-RU", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(isoString));
}

function formatMonthShort(value) {
  return new Intl.DateTimeFormat("ru-RU", {
    month: "short",
  })
    .format(new Date(`${value}-01T00:00:00Z`))
    .replace(".", "");
}

function formatMonthAxis(value) {
  const label = formatMonthShort(value);
  return label.charAt(0).toUpperCase() + label.slice(1);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

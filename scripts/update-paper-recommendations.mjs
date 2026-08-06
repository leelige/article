#!/usr/bin/env node

import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const OUTPUT = process.env.PAPER_RECOMMENDATIONS_OUTPUT
  ? pathToFileURL(resolve(process.cwd(), process.env.PAPER_RECOMMENDATIONS_OUTPUT))
  : new URL("../paper-recommendations/index.html", import.meta.url);

const SOURCES = [
  {
    name: "README",
    path: "README.md",
    url: "https://raw.githubusercontent.com/leelige/article/master/README.md",
  },
  {
    name: "Cost Model",
    path: "docs/Cost Model/Cost Model.md",
    url: "https://raw.githubusercontent.com/leelige/article/master/docs/Cost%20Model/Cost%20Model.md",
  },
  {
    name: "Database Tuning",
    path: "docs/Database Tuning/Database Tuning.md",
    url: "https://raw.githubusercontent.com/leelige/article/master/docs/Database%20Tuning/Database%20Tuning.md",
  },
  {
    name: "Knob Tuning",
    path: "docs/Knob Tuning/Knob Tuning.md",
    url: "https://raw.githubusercontent.com/leelige/article/master/docs/Knob%20Tuning/Knob%20Tuning.md",
  },
  {
    name: "Query Optimization",
    path: "docs/Query Optimization/Query Optimization.md",
    url: "https://raw.githubusercontent.com/leelige/article/master/docs/Query%20Optimization/Query%20Optimization.md",
  },
];

const SECTIONS = [
  {
    id: "optimizer",
    title: "查询优化 / 成本模型",
    description: "聚焦 cardinality estimation、查询重写、算子优化、物化视图和 workload-level 性能建模。",
    match: [
      "cardinality",
      "query optimization",
      "query optimizer",
      "query rewriting",
      "query rewrite",
      "cost model",
      "operator optimization",
      "materialized view",
      "semantic predicate",
      "semantic operator",
      "plan-level repair",
    ],
  },
  {
    id: "text2sql",
    title: "Text-to-SQL / NL-to-DB",
    description: "聚焦自然语言数据库接口，包括 SQL/Cypher 生成、schema grounding、plan repair 和交互式 SQL agent。",
    match: [
      "text-to-sql",
      "text2sql",
      "nl-to-sql",
      "natural-language-to-sql",
      "text-to-cypher",
      "text2cypher",
      "cypher",
      "sql agent",
      "sql generation",
      "schema",
      "natural language",
    ],
  },
  {
    id: "tuning",
    title: "调优 / 重配置 / 索引",
    description: "聚焦数据库自动调优、性能重配置、ANN 索引、learned index 和配置优化。",
    match: [
      "tuning",
      "reconfiguration",
      "configuration",
      "knob",
      "ann index",
      "index optimization",
      "learned index",
      "indexes",
      "indexing",
      "hnsw",
      "vector search",
    ],
  },
  {
    id: "storage",
    title: "存储 / 向量库 / 数据布局",
    description: "补充更偏基础设施的一组条目，覆盖 lakehouse、向量数据库、数据布局、数据库迁移和定制数据库生成。",
    match: [
      "lakehouse",
      "vector database",
      "vector databases",
      "data allocation",
      "data placement",
      "database migration",
      "postgresql",
      "oracle",
      "distributed database",
      "customized database",
      "customized databases",
      "storage",
      "hnsw",
    ],
  },
];

const DB_KEYWORDS = [
  "database",
  "dbms",
  "sql",
  "query",
  "cardinality",
  "optimizer",
  "optimization",
  "cost model",
  "materialized view",
  "lakehouse",
  "data allocation",
  "data placement",
  "transaction",
  "index",
  "indexes",
  "indexing",
  "learned index",
  "vector database",
  "hnsw",
  "postgresql",
  "oracle",
  "tuning",
  "knob",
  "configuration",
  "reconfiguration",
  "text-to-sql",
  "text2sql",
  "cypher",
  "schema",
  "relational",
  "storage",
  "workload",
  "operator",
  "semantic predicate",
  "semantic operator",
];

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function stripMarkdown(value) {
  return String(value)
    .replace(/\*\*/g, "")
    .replace(/`/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractLink(value) {
  const match = String(value).match(/\[[^\]]+\]\(([^)]+)\)/);
  return match?.[1] ?? "";
}

function parseMarkdownRows(markdown, source) {
  const rows = [];
  for (const line of markdown.split(/\r?\n/)) {
    if (!line.startsWith("|**")) continue;
    const cells = line.split("|").map((cell) => cell.trim());
    if (cells.length < 6) continue;

    const published = stripMarkdown(cells[1]);
    const title = stripMarkdown(cells[2]);
    const author = stripMarkdown(cells[3]);
    const pdf = extractLink(cells[4]);
    const code = extractLink(cells[5]);

    if (!published || !title || !pdf) continue;
    rows.push({
      published,
      title,
      author,
      pdf,
      code,
      source,
    });
  }
  return rows;
}

function parseDate(value) {
  const normalized = value.trim().replace(" ", "T");
  const date = new Date(`${normalized}+08:00`);
  if (Number.isNaN(date.getTime())) return new Date(0);
  return date;
}

function displayTime(value) {
  const [date, time = ""] = value.split(" ");
  return `${date} ${time.slice(0, 5)}`.trim();
}

function scorePaper(paper) {
  const text = `${paper.title} ${paper.source}`.toLowerCase();
  let score = 0;
  for (const keyword of DB_KEYWORDS) {
    if (text.includes(keyword)) score += keyword.includes("database") || keyword.includes("sql") ? 3 : 1;
  }
  if (/cs\.db/i.test(paper.title)) score += 2;
  return score;
}

function classifyPaper(paper) {
  const text = `${paper.title} ${paper.source}`.toLowerCase();
  const best = SECTIONS
    .map((section) => ({
      section,
      score: section.match.reduce((sum, keyword) => sum + (text.includes(keyword) ? 1 : 0), 0),
    }))
    .sort((a, b) => b.score - a.score)[0];

  if (best?.score > 0) return best.section;
  if (text.includes("database")) return SECTIONS[3];
  return SECTIONS[0];
}

function directionLabel(paper) {
  const text = paper.title.toLowerCase();
  if (text.includes("text-to-sql") || text.includes("text2sql")) return "Text-to-SQL";
  if (text.includes("cypher")) return "Text-to-Cypher";
  if (text.includes("cardinality")) return "Cardinality Estimation";
  if (text.includes("query rewriting") || text.includes("query rewrite")) return "Query Rewriting";
  if (text.includes("materialized view")) return "Materialized Views";
  if (text.includes("learned index")) return "Learned Index";
  if (text.includes("ann index") || text.includes("vector")) return "Vector DB / ANN";
  if (text.includes("tuning") || text.includes("configuration") || text.includes("reconfiguration")) return "Database Tuning";
  if (text.includes("migration") || text.includes("postgresql") || text.includes("oracle")) return "Database Migration";
  if (text.includes("lakehouse")) return "Lakehouse";
  if (text.includes("data allocation") || text.includes("data placement")) return "Data Placement";
  if (text.includes("cost")) return "Cost Model";
  if (text.includes("operator")) return "Operator Optimization";
  return "Database Systems";
}

function introFor(paper) {
  const text = paper.title.toLowerCase();
  if (text.includes("text-to-sql") || text.includes("text2sql")) {
    return "围绕自然语言到 SQL 的生成、编排或修复展开，关注如何让模型更稳定地理解 schema、规划步骤并生成可执行查询。";
  }
  if (text.includes("cypher")) {
    return "面向图数据库查询生成，讨论如何把自然语言问题可靠地转换成 Cypher，并减少 schema 与语法不一致。";
  }
  if (text.includes("cardinality")) {
    return "聚焦基数估计问题，试图为优化器提供更可靠的行数估计或评测基准。";
  }
  if (text.includes("query rewriting") || text.includes("query rewrite")) {
    return "研究查询重写与优化流程，强调在复杂 SQL 场景中提升改写收益与鲁棒性。";
  }
  if (text.includes("materialized view")) {
    return "研究物化视图选择与 workload 加速，重点在收益、维护成本和搜索策略之间做权衡。";
  }
  if (text.includes("learned index") || text.includes("index")) {
    return "围绕索引结构或索引调优展开，关注真实存储、缓存或检索性能下的成本收益。";
  }
  if (text.includes("tuning") || text.includes("configuration") || text.includes("reconfiguration")) {
    return "研究数据库或系统配置的自动调优，把 workload、历史反馈和模型建议转成可执行的配置动作。";
  }
  if (text.includes("lakehouse")) {
    return "研究云上 lakehouse 查询性能波动，分析影响查询运行时间预测的系统因素。";
  }
  if (text.includes("migration") || text.includes("postgresql") || text.includes("oracle")) {
    return "面向数据库迁移与 SQL 方言转换，关注语义保持、上下文组织和迁移成本。";
  }
  if (text.includes("data allocation") || text.includes("data placement")) {
    return "研究分布式数据库中的数据放置问题，平衡数据均衡、通信开销和迁移成本。";
  }
  return "这篇论文与数据库系统或数据管理任务相关，适合作为近期方向扫描中的候选阅读。";
}

function whyFor(paper) {
  const label = directionLabel(paper);
  return `它切中 ${label} 的一个具体问题，适合快速判断近期数据库相关研究在方法和系统落点上的变化。`;
}

function relevanceFor(paper) {
  const label = directionLabel(paper);
  return `数据库相关性：${label}，可用于跟踪数据库系统、查询处理、数据管理或 LLM-for-DB 的最新进展。`;
}

async function fetchText(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch(url, {
      headers: { "user-agent": "paper-recommendation-site-updater" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Fetch failed ${response.status}: ${url}`);
    return await response.text();
  } catch (error) {
    return fetchTextWithCurl(url, error);
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchTextWithCurl(url, originalError) {
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const execFileAsync = promisify(execFile);
  try {
    const { stdout } = await execFileAsync("curl", ["-L", "--max-time", "20", url], {
      maxBuffer: 8 * 1024 * 1024,
    });
    return stdout;
  } catch (curlError) {
    throw new Error(`Fetch failed for ${url}: ${originalError.message}; curl fallback failed: ${curlError.message}`);
  }
}

async function loadSourceMarkdown(source) {
  const localPath = resolve(process.cwd(), source.path);
  if (existsSync(localPath)) return readFile(localPath, "utf8");
  return fetchText(source.url);
}

async function loadPapers() {
  const fetched = await Promise.all(
    SOURCES.map(async (source) => {
      try {
        const markdown = await loadSourceMarkdown(source);
        return parseMarkdownRows(markdown, source.name);
      } catch (error) {
        console.warn(`[warn] ${error.message}`);
        return [];
      }
    }),
  );

  const byKey = new Map();
  for (const paper of fetched.flat()) {
    const key = paper.pdf || paper.title;
    const existing = byKey.get(key);
    if (!existing || parseDate(paper.published) > parseDate(existing.published)) {
      byKey.set(key, {
        ...paper,
        sources: Array.from(new Set([...(existing?.sources ?? []), paper.source])),
      });
    } else {
      existing.sources = Array.from(new Set([...(existing.sources ?? []), paper.source]));
    }
  }

  return Array.from(byKey.values())
    .map((paper) => ({
      ...paper,
      score: scorePaper(paper),
      section: classifyPaper(paper),
    }))
    .filter((paper) => paper.score >= 2)
    .sort((a, b) => parseDate(b.published) - parseDate(a.published) || b.score - a.score);
}

function selectPapers(papers) {
  const bySection = new Map(SECTIONS.map((section) => [section.id, []]));
  for (const paper of papers) {
    const bucket = bySection.get(paper.section.id);
    if (bucket && bucket.length < 5) bucket.push(paper);
  }

  const selected = Array.from(bySection.values()).flat();
  const top = selected
    .slice()
    .sort((a, b) => parseDate(b.published) - parseDate(a.published) || b.score - a.score)
    .slice(0, 5);

  return { bySection, top };
}

function renderPriority(papers) {
  return papers
    .map(
      (paper, index) => `
          <div class="priority-item">
            <span class="priority-rank">${index + 1}</span>
            <span class="priority-content">
              <time class="priority-date" datetime="${escapeHtml(paper.published)}">${escapeHtml(displayTime(paper.published))}</time>
              <a href="${escapeHtml(paper.pdf)}">${escapeHtml(paper.title)}</a>
              <span class="priority-note">${escapeHtml(whyFor(paper))}</span>
            </span>
          </div>`,
    )
    .join("\n");
}

function renderPaperCard(paper) {
  const direction = directionLabel(paper);
  return `
          <article class="paper-card">
            <div class="paper-top">
              <div>
                <h3><a href="${escapeHtml(paper.pdf)}">${escapeHtml(paper.title)}</a></h3>
                <div class="paper-meta">
                  <time class="paper-date" datetime="${escapeHtml(paper.published)}">时间：${escapeHtml(displayTime(paper.published))}</time>
                  <span class="tag db">研究方向：${escapeHtml(direction)}</span>
                </div>
              </div>
              <div class="tag-row"><span class="tag sys">${escapeHtml(paper.source)}</span><span class="tag llm">推荐阅读</span></div>
            </div>
            <p>${escapeHtml(introFor(paper))}</p>
            <div class="reason"><strong>为什么值得读：</strong>${escapeHtml(whyFor(paper))}<br><strong>${escapeHtml(relevanceFor(paper))}</strong></div>
          </article>`;
}

function renderSection(section, papers) {
  if (papers.length === 0) return "";
  return `
    <section class="section" id="${section.id}">
      <div class="section-card">
        <div class="section-head">
          <h2>${escapeHtml(section.title)}</h2>
          <p>${escapeHtml(section.description)}</p>
        </div>
        <div class="cards">
${papers.map(renderPaperCard).join("\n")}
        </div>
      </div>
    </section>`;
}

function todayShanghai() {
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  })
    .format(new Date())
    .replaceAll("/", "-");
}

function renderHtml({ bySection, top }) {
  const sectionsHtml = SECTIONS.map((section) => renderSection(section, bySection.get(section.id) ?? [])).join("\n");
  const updated = todayShanghai();

  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>leelige/article 每日论文推荐</title>
  <meta name="description" content="从 leelige/article 自动整理的数据库相关论文推荐。">
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fb;
      --panel: #ffffff;
      --ink: #16202a;
      --muted: #657383;
      --line: #dbe2ea;
      --blue: #2364aa;
      --green: #0e7c66;
      --violet: #6652c7;
      --shadow: 0 16px 44px rgba(21, 32, 43, 0.09);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      line-height: 1.62;
    }
    a { color: var(--blue); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .hero {
      background: #182333;
      color: #fff;
      border-bottom: 1px solid rgba(255, 255, 255, 0.12);
    }
    .hero-inner, .nav, main {
      width: min(1180px, calc(100% - 40px));
      margin: 0 auto;
    }
    .hero-inner { padding: 34px 0 28px; }
    .eyebrow {
      margin: 0 0 8px;
      color: #a9c8ec;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      max-width: 920px;
      font-size: clamp(30px, 5vw, 58px);
      line-height: 1.08;
      letter-spacing: 0;
    }
    .subtitle {
      margin: 18px 0 0;
      max-width: 850px;
      color: #d9e4f2;
      font-size: 17px;
    }
    .meta-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 22px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 5px 11px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      border-radius: 999px;
      color: #eef5ff;
      font-size: 13px;
      font-weight: 650;
      white-space: nowrap;
    }
    .nav-wrap {
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(247, 248, 251, 0.96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(14px);
    }
    .nav {
      display: flex;
      gap: 8px;
      overflow-x: auto;
      padding: 10px 0;
    }
    .nav a {
      flex: 0 0 auto;
      min-height: 36px;
      padding: 7px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: #263241;
      font-size: 14px;
      font-weight: 650;
    }
    main { padding: 28px 0 56px; }
    .summary-card, .section-card, .footer-card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .summary-card { padding: 20px; }
    .summary-card p, .section-head p, .footer-card p { margin: 0; color: var(--muted); }
    h2 {
      margin: 0 0 14px;
      font-size: 24px;
      line-height: 1.25;
      letter-spacing: 0;
    }
    .priority-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .priority-item {
      display: grid;
      grid-template-columns: 28px 1fr;
      gap: 10px;
      align-items: start;
      color: #263241;
    }
    .priority-rank {
      width: 28px;
      height: 28px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      background: #edf4ff;
      color: var(--blue);
      font-weight: 800;
      font-size: 13px;
    }
    .priority-content { display: grid; gap: 2px; }
    .priority-date {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .priority-note { color: #445260; font-size: 14px; }
    .section { margin-top: 30px; scroll-margin-top: 78px; }
    .section-card { padding: 20px; }
    .section-head {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 18px;
      margin-bottom: 14px;
      border-bottom: 2px solid var(--line);
      padding-bottom: 10px;
    }
    .section-head p { max-width: 640px; font-size: 14px; }
    .cards {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .paper-card {
      display: flex;
      flex-direction: column;
      gap: 12px;
      min-height: 286px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
      box-shadow: 0 8px 26px rgba(21, 32, 43, 0.06);
    }
    .paper-top {
      display: flex;
      gap: 10px;
      align-items: flex-start;
      justify-content: space-between;
    }
    h3 {
      margin: 0;
      font-size: 18px;
      line-height: 1.35;
      letter-spacing: 0;
    }
    .paper-card h3 a { color: var(--ink); }
    .paper-meta, .tag-row {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
      margin-top: 8px;
    }
    .tag-row { margin-top: 0; }
    .tag, .paper-date {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 3px 8px;
      border-radius: 6px;
      background: #eef2f7;
      color: #3f4b59;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }
    .tag.db { background: #e7f6f1; color: var(--green); }
    .tag.sys { background: #edf4ff; color: var(--blue); }
    .tag.llm { background: #f1effc; color: var(--violet); }
    .paper-date { background: #f3f6fa; color: #516171; }
    .paper-card p { margin: 0; color: #445260; }
    .reason {
      margin-top: auto;
      padding-top: 12px;
      border-top: 1px solid var(--line);
      color: #2f3c49;
      font-size: 14px;
    }
    .reason strong { color: var(--ink); }
    .footer-card {
      margin-top: 34px;
      padding: 20px;
      color: var(--muted);
      font-size: 14px;
    }
    @media (max-width: 840px) {
      .hero-inner, .nav, main { width: min(100% - 28px, 1180px); }
      .cards { grid-template-columns: 1fr; }
      .section-head { display: block; }
      .section-head p { margin-top: 8px; }
    }
    @media (max-width: 520px) {
      .hero-inner { padding: 26px 0 22px; }
      .paper-top { display: block; }
      .tag-row { margin-top: 10px; }
      .paper-card { min-height: auto; }
    }
  </style>
</head>
<body>
  <header class="hero">
    <div class="hero-inner">
      <p class="eyebrow">leelige/article monitor</p>
      <h1>每日数据库相关论文推荐</h1>
      <p class="subtitle">自动从 leelige/article 公开内容中筛选数据库、数据系统、Text-to-SQL、查询优化、索引和调优相关论文。网页由独立脚本更新，不依赖 Codex App 自动化。</p>
      <div class="meta-row">
        <span class="pill">更新日期：${escapeHtml(updated)}</span>
        <span class="pill">来源：GitHub + arXiv 链接</span>
        <span class="pill">重点：数据库相关</span>
      </div>
    </div>
  </header>

  <div class="nav-wrap">
    <nav class="nav" aria-label="报告导航">
      <a href="#priority">优先读</a>
${SECTIONS.map((section) => `      <a href="#${section.id}">${escapeHtml(section.title)}</a>`).join("\n")}
      <a href="#sources">来源</a>
    </nav>
  </div>

  <main>
    <section id="priority">
      <div class="summary-card">
        <h2>今天先读这 5 篇</h2>
        <p>按论文发布时间从新到旧排序，优先展示当前筛选结果中最新且数据库相关性较强的条目。</p>
        <div class="priority-list">
${renderPriority(top)}
        </div>
      </div>
    </section>

${sectionsHtml}

    <section class="footer-card" id="sources">
      <p><strong>来源：</strong><a href="https://github.com/leelige/article">leelige/article</a>。本页由 <code>scripts/update-paper-recommendations.mjs</code> 自动生成；不会创建 Codex App 对话。每个方向最多保留 5 篇，方向内部按发布时间倒序排列。</p>
    </section>
  </main>
</body>
</html>
`;
}

async function main() {
  const papers = await loadPapers();
  if (papers.length === 0) {
    throw new Error("No database-related papers found. Refusing to overwrite the site.");
  }
  const selected = selectPapers(papers);
  const html = renderHtml(selected);
  const outputPath = fileURLToPath(OUTPUT);
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, html, "utf8");
  console.log(`updated ${outputPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

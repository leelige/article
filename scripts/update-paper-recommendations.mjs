#!/usr/bin/env node

import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const OUTPUT = process.env.PAPER_RECOMMENDATIONS_OUTPUT
  ? pathToFileURL(resolve(process.cwd(), process.env.PAPER_RECOMMENDATIONS_OUTPUT))
  : new URL("../paper-recommendations/index.html", import.meta.url);

const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5.6-luna";
const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const OPENAI_MAX_ATTEMPTS = 3;
const BANNED_GENERATED_PHRASES = [
  "跟踪最新进展",
  "适合作为候选阅读",
  "适合快速判断",
  "一个具体问题",
];

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

function arxivIdFor(paper) {
  try {
    const path = decodeURIComponent(new URL(paper.pdf).pathname);
    const match = path.match(/^\/(?:abs|pdf)\/(.+?)(?:\.pdf)?\/?$/i);
    return match?.[1] ?? "";
  } catch {
    return "";
  }
}

function baseArxivId(id) {
  return String(id).replace(/v\d+$/i, "");
}

function decodeXml(value) {
  return String(value)
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCodePoint(Number.parseInt(code, 16)))
    .replace(/&#(\d+);/g, (_, code) => String.fromCodePoint(Number.parseInt(code, 10)))
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&apos;", "'")
    .replaceAll("&amp;", "&")
    .replace(/\s+/g, " ")
    .trim();
}

function xmlTag(block, name) {
  const match = block.match(new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, "i"));
  return match ? decodeXml(match[1]) : "";
}

function parseArxivFeed(xml) {
  const abstracts = new Map();
  for (const match of xml.matchAll(/<entry(?:\s[^>]*)?>([\s\S]*?)<\/entry>/gi)) {
    const entry = match[1];
    const id = xmlTag(entry, "id").split("/abs/")[1] ?? "";
    const summary = xmlTag(entry, "summary");
    if (!id || !summary) continue;
    abstracts.set(id, summary);
    abstracts.set(baseArxivId(id), summary);
  }
  return abstracts;
}

function normalizeDescription(value) {
  return String(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\p{P}\p{S}\s]+/gu, "");
}

function truncateText(value, maxLength = 220) {
  const text = String(value).trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1).trimEnd()}…`;
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

async function attachArxivAbstracts(papers) {
  const ids = Array.from(new Set(papers.map(arxivIdFor).filter(Boolean)));
  if (ids.length === 0) {
    throw new Error("No arXiv IDs found; Chinese recommendations cannot be generated.");
  }

  try {
    const endpoint = new URL("https://export.arxiv.org/api/query");
    endpoint.searchParams.set("id_list", ids.join(","));
    endpoint.searchParams.set("max_results", String(ids.length));
    const abstracts = parseArxivFeed(await fetchText(endpoint.toString()));
    let attached = 0;

    for (const paper of papers) {
      const id = arxivIdFor(paper);
      const abstract = abstracts.get(id) ?? abstracts.get(baseArxivId(id)) ?? "";
      if (!abstract) continue;
      paper.abstract = abstract;
      attached += 1;
    }
    console.log(`[info] arXiv abstracts: ${attached}/${papers.length}`);
  } catch (error) {
    throw new Error(`Unable to load arXiv abstracts: ${error.message}`);
  }
}

function outputTextFromResponse(response) {
  if (typeof response.output_text === "string") return response.output_text;
  for (const item of response.output ?? []) {
    if (item.type !== "message") continue;
    for (const content of item.content ?? []) {
      if (content.type === "output_text" && typeof content.text === "string") return content.text;
    }
  }
  return "";
}

function isValidGeneratedText(value, minLength) {
  const text = String(value ?? "").trim();
  return (
    text.length >= minLength &&
    text.length <= 500 &&
    /\p{Script=Han}/u.test(text) &&
    !BANNED_GENERATED_PHRASES.some((phrase) => text.includes(phrase))
  );
}

function validatedRecommendations(value, expectedRecords) {
  if (!Array.isArray(value?.papers)) return new Map();

  const expectedIds = new Set(expectedRecords.map(({ id }) => id));
  const candidates = [];
  const seenIds = new Set();
  for (const item of value.papers) {
    const id = String(item?.id ?? "");
    if (!expectedIds.has(id) || seenIds.has(id)) continue;
    seenIds.add(id);
    candidates.push({
      id,
      intro: String(item.intro ?? "").trim(),
      why: String(item.why ?? "").trim(),
      relevance: String(item.relevance ?? "").trim(),
    });
  }

  const duplicateCounts = new Map();
  for (const field of ["intro", "why", "relevance"]) {
    const counts = new Map();
    for (const item of candidates) {
      const normalized = normalizeDescription(item[field]);
      counts.set(normalized, (counts.get(normalized) ?? 0) + 1);
    }
    duplicateCounts.set(field, counts);
  }

  const valid = new Map();
  for (const item of candidates) {
    const fieldsAreValid =
      isValidGeneratedText(item.intro, 24) &&
      isValidGeneratedText(item.why, 16) &&
      isValidGeneratedText(item.relevance, 16);
    const fieldsAreUnique = ["intro", "why", "relevance"].every(
      (field) => duplicateCounts.get(field).get(normalizeDescription(item[field])) === 1,
    );
    if (!fieldsAreValid || !fieldsAreUnique) continue;
    valid.set(item.id, { ...item, source: "openai", model: OPENAI_MODEL });
  }
  return valid;
}

async function openAiHttpError(response) {
  let code = "";
  let message = "";
  try {
    const body = await response.json();
    code = String(body?.error?.code ?? body?.error?.type ?? "");
    message = String(body?.error?.message ?? "");
  } catch {
    // The status code still identifies the failure when the body is not JSON.
  }

  const details = [code, message].filter(Boolean).join(": ");
  const error = new Error(`OpenAI API returned HTTP ${response.status}${details ? ` (${details})` : ""}`);
  error.status = response.status;
  error.code = code;
  return error;
}

async function requestOpenAi(apiKey, body) {
  for (let attempt = 1; attempt <= OPENAI_MAX_ATTEMPTS; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 90000);
    try {
      const response = await fetch(OPENAI_RESPONSES_URL, {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (response.ok) return await response.json();

      const error = await openAiHttpError(response);
      const canRetry =
        error.status === 429 &&
        !["insufficient_quota", "billing_hard_limit_reached", "credit_balance_exhausted"].includes(error.code) &&
        attempt < OPENAI_MAX_ATTEMPTS;
      if (!canRetry) throw error;

      const delayMs = attempt * 5000;
      console.warn(`[warn] ${error.message}; retrying in ${delayMs / 1000}s.`);
      await new Promise((resolveDelay) => setTimeout(resolveDelay, delayMs));
    } finally {
      clearTimeout(timeout);
    }
  }
  throw new Error("OpenAI API request exhausted all retry attempts.");
}

async function generateOpenAiRecommendations(records) {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not configured; refusing to publish English fallback text.");
  }

  const recordsWithAbstracts = records.filter(({ paper }) => paper.abstract);
  if (recordsWithAbstracts.length !== records.length) {
    throw new Error(
      `Only ${recordsWithAbstracts.length}/${records.length} arXiv abstracts are available; refusing to publish incomplete Chinese recommendations.`,
    );
  }

  const schema = {
    type: "object",
    properties: {
      papers: {
        type: "array",
        items: {
          type: "object",
          properties: {
            id: { type: "string" },
            intro: { type: "string" },
            why: { type: "string" },
            relevance: { type: "string" },
          },
          required: ["id", "intro", "why", "relevance"],
          additionalProperties: false,
        },
      },
    },
    required: ["papers"],
    additionalProperties: false,
  };
  const input = {
    papers: recordsWithAbstracts.map(({ id, paper }) => ({
      id,
      title: paper.title,
      abstract: paper.abstract,
      direction: directionLabel(paper),
    })),
  };
  const instructions = `你是数据库研究论文编辑。根据每篇论文的标题和 arXiv 摘要，生成简洁、具体、忠于原文的中文介绍。

对输入中的每篇论文都返回一项，并原样保留 id：
- intro：用 1 至 2 句说明研究问题和核心方法。
- why：用 1 句指出具体创新、实验发现或系统价值；摘要没有的信息不要补写。
- relevance：用 1 句明确说明它与数据库研究的联系，具体到查询优化、成本模型、索引、调优、存储、事务或 NL-to-DB 等对象。

同一批次各论文的三个字段都不能出现完全相同的表述。不要使用“跟踪最新进展”“适合作为候选阅读”“适合快速判断”“一个具体问题”等泛化句式。输出必须以中文为主，技术名词可以保留英文。不要添加字段标签。`;

  const responseBody = await requestOpenAi(apiKey, {
    model: OPENAI_MODEL,
    instructions,
    input: JSON.stringify(input),
    text: {
      format: {
        type: "json_schema",
        name: "paper_recommendations",
        strict: true,
        schema,
      },
    },
    max_output_tokens: 7000,
    store: false,
  });
  const outputText = outputTextFromResponse(responseBody);
  if (!outputText) throw new Error("OpenAI API returned no text output.");

  const generated = validatedRecommendations(JSON.parse(outputText), recordsWithAbstracts);
  if (generated.size !== records.length) {
    throw new Error(
      `Only ${generated.size}/${records.length} OpenAI summaries passed Chinese and uniqueness checks; refusing to deploy.`,
    );
  }
  console.log(`[info] OpenAI summaries accepted: ${generated.size}/${records.length}`);
  return generated;
}

async function enrichRecommendations(selection) {
  const papers = Array.from(new Set(Array.from(selection.bySection.values()).flat()));
  const records = papers.map((paper, index) => ({ id: `paper-${index + 1}`, paper }));
  await attachArxivAbstracts(papers);
  const generated = await generateOpenAiRecommendations(records);

  for (const { id, paper } of records) {
    paper.recommendation = generated.get(id);
  }
  console.log(`[info] Recommendation sources: OpenAI ${papers.length}, English fallback 0`);
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
              <span class="priority-note">${escapeHtml(truncateText(paper.recommendation.why))}</span>
            </span>
          </div>`,
    )
    .join("\n");
}

function renderPaperCard(paper) {
  const direction = directionLabel(paper);
  const recommendation = paper.recommendation;
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
              <div class="tag-row"><span class="tag sys">${escapeHtml(paper.source)}</span><span class="tag llm">AI 中文介绍</span></div>
            </div>
            <p>${escapeHtml(recommendation.intro)}</p>
            <div class="reason"><strong>为什么值得读：</strong>${escapeHtml(recommendation.why)}<br><strong>数据库相关性：</strong>${escapeHtml(recommendation.relevance)}</div>
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
  const contentMode = `中文介绍：${OPENAI_MODEL}`;

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
        <span class="pill">${escapeHtml(contentMode)}</span>
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
      <p><strong>来源：</strong><a href="https://github.com/leelige/article">leelige/article</a> 与论文 arXiv 摘要。本页由 <code>scripts/update-paper-recommendations.mjs</code> 自动生成；不会创建 Codex App 对话。仅在全部逐篇中文介绍生成并通过去重检查后发布，不展示英文摘要回退。每个方向最多保留 5 篇，方向内部按发布时间倒序排列。</p>
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
  await enrichRecommendations(selected);
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

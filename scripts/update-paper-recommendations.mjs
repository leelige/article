#!/usr/bin/env node

import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const OUTPUT = process.env.PAPER_RECOMMENDATIONS_OUTPUT
  ? pathToFileURL(resolve(process.cwd(), process.env.PAPER_RECOMMENDATIONS_OUTPUT))
  : new URL("../paper-recommendations/index.html", import.meta.url);

const RULE_SUMMARIZER_MODEL = "database-rules-v1";
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
      "semantic query",
      "semantic queries",
      "semantic join",
      "query execution",
      "in-database",
      "prompt management",
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
      "query generation",
      "schema retrieval",
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
      "vector search",
      "olap",
      "graph database",
      "knowledge graph",
      "data analytics",
      "disaggregated storage",
      "heterogeneous storage",
      "data lake",
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

const DB_TITLE_SIGNALS = [
  { needles: ["database", "dbms", "sql"], weight: 4 },
  { needles: ["cardinality", "materialized view", "semantic join", "semantic operator"], weight: 4 },
  { needles: ["query optimization", "query optimizer", "query rewriting", "query execution", "query cost"], weight: 4 },
  { needles: ["text-to-cypher", "text2cypher", "cypher", "schema retrieval"], weight: 4 },
  { needles: ["learned index", "vector database", "vector search", "hnsw", "olap", "lakehouse"], weight: 4 },
  { needles: ["postgresql", "group commit", "data allocation", "data placement", "data lake"], weight: 3 },
  { needles: ["relational data", "semantic query", "semantic queries", "virtual knowledge graph", "disaggregated storage", "heterogeneous storage"], weight: 3 },
  { needles: ["query generation", "data analytics"], weight: 2 },
];

const TOPIC_RULES = [
  { needles: ["text-to-sql", "text2sql", "nl-to-sql", "sql generation"], label: "Text-to-SQL", impact: "自然语言意图、数据库模式与可执行 SQL 之间的对齐" },
  { needles: ["text-to-cypher", "text2cypher", "cypher"], label: "Text-to-Cypher", impact: "自然语言到图查询语言的结构映射与可执行性" },
  { needles: ["cardinality"], label: "基数估计", impact: "估计误差、代价计算与执行计划选择" },
  { needles: ["query rewrite", "query rewriting"], label: "查询重写", impact: "等价变换、搜索空间控制与计划质量" },
  { needles: ["query execution"], label: "查询执行引擎", impact: "执行代码生成、跨引擎可移植性与运行时开销" },
  { needles: ["semantic join"], label: "语义连接优化", impact: "语义连接算子组织、模型调用开销与执行计划质量" },
  { needles: ["semantic query", "semantic queries"], label: "语义查询优化", impact: "语义算子计划空间、模型调用成本与端到端延迟" },
  { needles: ["materialized view"], label: "物化视图", impact: "查询复用收益、视图覆盖范围与维护代价" },
  { needles: ["vector database", "vector search", "ann index", "nearest neighbor", "hnsw"], label: "向量检索", impact: "召回质量、索引参数与查询延迟之间的权衡" },
  { needles: ["learned index"], label: "学习型索引", impact: "数据分布建模、查找路径与索引维护开销" },
  { needles: ["index optimization", "indexing", "indexes", "index"], label: "索引优化", impact: "访问路径选择、索引结构与读写开销" },
  { needles: ["knob", "database tuning", "configuration", "reconfiguration"], label: "数据库调优", impact: "配置搜索、性能反馈与工作负载适配" },
  { needles: ["in-database", "prompt management"], label: "数据库内模型管理", impact: "提示词对象的存储、版本管理、重写与查询内调用" },
  { needles: ["cost model"], label: "成本模型", impact: "算子代价建模与候选计划比较" },
  { needles: ["query optimization", "query optimizer", "query plan", "plan search"], label: "查询优化", impact: "候选计划生成、代价比较与执行策略选择" },
  { needles: ["lakehouse"], label: "Lakehouse", impact: "分析型数据管理中的存储组织与执行效率" },
  { needles: ["data layout", "data placement", "data allocation"], label: "数据布局", impact: "数据放置、访问局部性与资源开销" },
  { needles: ["database migration", "postgresql", "oracle"], label: "数据库迁移", impact: "跨系统语义保持、兼容性与迁移验证" },
  { needles: ["transaction", "concurrency control"], label: "事务处理", impact: "事务正确性、并发调度与执行效率" },
  { needles: ["storage", "distributed database"], label: "存储系统", impact: "数据组织、访问路径与系统资源消耗" },
  { needles: ["operator optimization", "semantic operator", "operator"], label: "数据库算子", impact: "算子实现、执行策略与端到端性能" },
  { needles: ["schema", "semantic predicate"], label: "模式与语义约束", impact: "模式匹配、结构约束与查询正确性" },
];

const METHOD_RULES = [
  { needles: ["lora", "low-rank adaptation"], label: "低秩适配" },
  { needles: ["quantization"], label: "模型量化" },
  { needles: ["knowledge distillation", "distillation"], label: "知识蒸馏" },
  { needles: ["embedding"], label: "向量表示学习" },
  { needles: ["calibration"], label: "代价校准" },
  { needles: ["gpu"], label: "GPU并行执行" },
  { needles: ["hnsw"], label: "图索引与参数搜索" },
  { needles: ["olap-native"], label: "分析查询与向量检索融合" },
  { needles: ["graph database", "knowledge graph"], label: "图数据建模" },
  { needles: ["self-clock"], label: "负载自适应控制" },
  { needles: ["large language model", "language model", " llm", "llm-"], label: "语言模型" },
  { needles: ["reinforcement learning"], label: "强化学习" },
  { needles: ["graph neural", "graph network"], label: "图神经网络" },
  { needles: ["transformer"], label: "Transformer 表示学习" },
  { needles: ["bayesian optimization"], label: "贝叶斯优化" },
  { needles: ["machine learning", "learned", "neural"], label: "学习模型" },
  { needles: ["workload-aware", "workload aware"], label: "工作负载感知建模" },
  { needles: ["adaptive", "dynamic"], label: "自适应策略" },
  { needles: ["sampling", "sample-based", "sketch"], label: "采样与数据摘要" },
  { needles: ["histogram"], label: "统计直方图" },
  { needles: ["semantic"], label: "语义表示与约束" },
  { needles: ["agent"], label: "智能体式推理" },
  { needles: ["search", "pruning"], label: "搜索与剪枝" },
  { needles: ["rule-based", "rules"], label: "规则推理" },
];

const CHALLENGE_RULES = [
  { needles: ["vulnerabil", "adversarial", "attack", "security"], label: "查询生成中的脆弱性与失效边界" },
  { needles: ["confidential", "trusted execution"], label: "机密执行环境带来的性能不确定性" },
  { needles: ["portable", "portability"], label: "跨执行引擎的可移植性" },
  { needles: ["constraint-aware", "constraint aware"], label: "约束条件下的质量与效率平衡" },
  { needles: ["correlation", "correlated", "data skew", "skewed"], label: "数据相关性与分布偏斜" },
  { needles: ["drift", "evolving", "changing workload", "dynamic workload"], label: "数据分布和工作负载变化" },
  { needles: ["scalability", "large-scale", "large scale", "billion"], label: "大规模场景下的计算与搜索开销" },
  { needles: ["schema linking", "schema grounding"], label: "自然语言意图与数据库模式对齐" },
  { needles: ["search space"], label: "计划或配置搜索空间过大" },
  { needles: ["migration", "compatibility", "compatible"], label: "跨数据库迁移中的兼容性和语义保持" },
  { needles: ["latency", "slow", "performance", "efficient"], label: "查询延迟与执行效率" },
  { needles: ["accuracy", "error", "robust", "reliability"], label: "结果准确性与鲁棒性" },
  { needles: ["memory", "storage overhead", "resource"], label: "存储和资源开销" },
];

const EVALUATION_RULES = [
  { needles: ["q-error", "q error"], label: "q-error" },
  { needles: ["latency"], label: "查询延迟" },
  { needles: ["throughput"], label: "系统吞吐量" },
  { needles: ["accuracy"], label: "准确率" },
  { needles: ["recall"], label: "召回率" },
  { needles: ["execution time", "runtime", "speedup", "faster"], label: "运行时间" },
  { needles: ["memory"], label: "内存开销" },
  { needles: ["benchmark", "datasets", "workloads"], label: "多数据集或工作负载" },
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
  const text = paper.title.toLowerCase();
  let score = 0;
  for (const signal of DB_TITLE_SIGNALS) {
    if (signal.needles.some((needle) => text.includes(needle))) score += signal.weight;
  }
  return score;
}

function classifyPaper(paper) {
  const text = paper.title.toLowerCase();
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
  if (text.includes("in-database") || text.includes("prompt management")) return "In-Database LLM";
  if (text.includes("query execution")) return "Query Execution";
  if (text.includes("semantic join") || text.includes("semantic quer")) return "Semantic Query Optimization";
  if (text.includes("schema retrieval")) return "Schema Retrieval";
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

function isValidGeneratedText(value, minLength) {
  const text = String(value ?? "").trim();
  const hanCount = text.match(/\p{Script=Han}/gu)?.length ?? 0;
  const latinCount = text.match(/[A-Za-z]/g)?.length ?? 0;
  return (
    text.length >= minLength &&
    text.length <= 500 &&
    hanCount >= 8 &&
    hanCount / Math.max(hanCount + latinCount, 1) >= 0.35 &&
    !BANNED_GENERATED_PHRASES.some((phrase) => text.includes(phrase))
  );
}

function validatedRecommendations(value, expectedRecords) {
  if (!Array.isArray(value?.papers)) return new Map();

  const expectedIds = new Set(expectedRecords.map(({ id }) => id));
  const expectedTitles = new Map(expectedRecords.map(({ id, paper }) => [id, paper.title]));
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
    const invalidFields = [
      ["intro", 24],
      ["why", 16],
      ["relevance", 16],
    ].filter(([field, minLength]) => !isValidGeneratedText(item[field], minLength)).map(([field]) => field);
    const duplicateFields = ["intro", "why", "relevance"].filter(
      (field) => duplicateCounts.get(field).get(normalizeDescription(item[field])) !== 1,
    );
    if (invalidFields.length > 0 || duplicateFields.length > 0) {
      console.warn(
        `[warn] Rejected summary for ${expectedTitles.get(item.id) ?? item.id}: ` +
        `invalid=${invalidFields.join(",") || "none"}; duplicate=${duplicateFields.join(",") || "none"}`,
      );
      continue;
    }
    valid.set(item.id, { ...item, source: "rules", model: RULE_SUMMARIZER_MODEL });
  }
  return valid;
}

function matchingRules(text, rules, limit = 2) {
  const lower = text.toLowerCase();
  return rules.filter((rule) => rule.needles.some((needle) => lower.includes(needle))).slice(0, limit);
}

function uniqueRules(rules, limit = rules.length) {
  const seen = new Set();
  return rules.filter((rule) => {
    if (seen.has(rule.label)) return false;
    seen.add(rule.label);
    return true;
  }).slice(0, limit);
}

function stableVariant(value, count) {
  let hash = 0;
  for (const character of value) hash = (hash * 31 + character.codePointAt(0)) >>> 0;
  return hash % count;
}

function extractNamedArtifact(title) {
  const prefix = title.split(":", 1)[0].trim();
  const generic = /^(a|an|the|towards?|rethinking|efficient|adaptive|scalable|learning|optimizing)$/i;
  const prefixWords = prefix.split(/\s+/);
  if (prefixWords.length <= 3 && prefix.length <= 42 && /[A-Z]/.test(prefix) && !generic.test(prefix)) return prefix;

  const candidates = title.match(/\b(?:[A-Z]{2,}[A-Za-z0-9-]*|[A-Z][a-z0-9]+[A-Z][A-Za-z0-9-]*)\b/g) ?? [];
  const excluded = new Set(["SQL", "DB", "DBMS", "LLM", "AI", "ANN", "HNSW", "NLP", "GPU"]);
  return candidates.find((candidate) => !excluded.has(candidate)) ?? "";
}

function extractEvidence(abstract) {
  const sentences = abstract.split(/(?<=[.!?])\s+(?=[A-Z0-9(])/);
  const evidenceCues = /\b(experiment|evaluation|result|outperform|improv|reduc|achiev|faster|speedup|latency|accuracy|recall|throughput)\w*/i;
  const sentence = sentences.find((candidate) => evidenceCues.test(candidate)) ?? abstract;
  const numbers = Array.from(sentence.matchAll(/\b\d+(?:\.\d+)?\s*(?:%|x|×|times?)\b/gi), (match) => match[0]).slice(0, 2);
  const evaluations = matchingRules(`${sentence} ${abstract}`, EVALUATION_RULES, 3).map((rule) => rule.label);
  const hasExperiment = /\b(experiment|evaluation|benchmark|empirical|results?)\w*/i.test(abstract);
  if (evaluations.length === 0 && hasExperiment) evaluations.push("对比实验");
  return { numbers, evaluations };
}

function fallbackTopic(paper) {
  return {
    optimizer: { label: "查询优化", impact: "代价估计、计划选择与查询执行效率" },
    text2sql: { label: "自然语言数据库接口", impact: "用户意图、数据库模式与可执行查询之间的映射" },
    tuning: { label: "数据库调优", impact: "配置搜索、性能反馈与工作负载适配" },
    storage: { label: "数据系统基础设施", impact: "数据组织、访问路径与资源开销" },
  }[paper.section.id];
}

function buildRuleRecommendation(paper) {
  const topics = matchingRules(paper.title, TOPIC_RULES, 3);
  if (topics.length === 0) topics.push(fallbackTopic(paper));
  const methods = matchingRules(paper.title, METHOD_RULES, 2).map((rule) => rule.label);
  const challenges = uniqueRules([
    ...matchingRules(paper.title, CHALLENGE_RULES, 2),
    ...matchingRules(paper.abstract, CHALLENGE_RULES, 3),
  ], 2).map((rule) => rule.label);
  const artifact = extractNamedArtifact(paper.title);
  const evidence = extractEvidence(paper.abstract);
  const variant = stableVariant(paper.title, 4);

  const topicText = topics.map((topic) => topic.label).join("、");
  const methodText = methods.length > 0 ? methods.join("、") : "结构化任务建模与系统实验";
  const challengeText = challenges[0] ?? `${topics[0].label}中的结果质量与系统效率平衡`;
  const introLeads = ["这项研究聚焦", "论文面向", "作者围绕", "该工作研究"];
  const contribution = artifact
    ? `作者提出“${artifact}”，以${methodText}改进${topics[0].impact}`
    : `作者采用${methodText}构建解决路径，直接作用于${topics[0].impact}`;
  const intro = `${introLeads[variant]}${topicText}，围绕${challengeText}展开。${contribution}。`;

  const evaluationText = evidence.evaluations.length > 0 ? evidence.evaluations.join("、") : "实验结果";
  const evaluatedObject = artifact ? `“${artifact}”` : (methods[0] ?? topics[0].label);
  const whyLeads = ["值得关注的是", "摘要中的关键证据是", "论文的实证重点是", "其具体价值体现在"];
  const why = evidence.numbers.length > 0
    ? `${whyLeads[variant]}，摘要通过${evaluationText}评估${evaluatedObject}，并给出${evidence.numbers.join("、")}等量化结果；这些结果可用于判断其在${topics[0].label}上的实际收益。`
    : `${whyLeads[variant]}，摘要通过${evaluationText}考察${evaluatedObject}面对${challengeText}时的表现；验证目标对应${topics[0].impact}。`;

  const relevanceLeads = ["从数据库研究角度看", "在数据系统中", "就数据库技术而言", "面向数据库工作负载"];
  const secondary = topics.slice(1).map((topic) => topic.label);
  const secondaryText = secondary.length > 0 ? `，并延伸到${secondary.join("、")}` : "";
  const impacts = uniqueRules(topics).map((topic) => topic.impact);
  const relevanceSubject = artifact ? `“${artifact}”` : "论文方案";
  const relevance = `${relevanceLeads[variant]}，该论文直接对应${topics[0].label}${secondaryText}；${relevanceSubject}采用${methodText}，关注${impacts.join("；")}。`;

  return { intro, why, relevance };
}

function generateRuleRecommendations(records) {
  const recordsWithAbstracts = records.filter(({ paper }) => paper.abstract);
  if (recordsWithAbstracts.length !== records.length) {
    throw new Error(
      `Only ${recordsWithAbstracts.length}/${records.length} arXiv abstracts are available; refusing to publish incomplete Chinese recommendations.`,
    );
  }

  const value = {
    papers: recordsWithAbstracts.map(({ id, paper }) => ({
      id,
      ...buildRuleRecommendation(paper),
    })),
  };

  const generated = validatedRecommendations(value, recordsWithAbstracts);
  if (generated.size !== records.length) {
    throw new Error(
      `Only ${generated.size}/${records.length} rule summaries passed Chinese and uniqueness checks; refusing to deploy.`,
    );
  }
  console.log(`[info] Zero-dependency Chinese summaries accepted: ${generated.size}/${records.length}`);
  return generated;
}

async function enrichRecommendations(selection) {
  const papers = Array.from(new Set(Array.from(selection.bySection.values()).flat()));
  const records = papers.map((paper, index) => ({ id: `paper-${index + 1}`, paper }));
  await attachArxivAbstracts(papers);
  const generated = generateRuleRecommendations(records);

  for (const { id, paper } of records) {
    paper.recommendation = generated.get(id);
  }
  console.log(`[info] Recommendation sources: zero-dependency rules ${papers.length}, English fallback 0`);
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
              <div class="tag-row"><span class="tag sys">${escapeHtml(paper.source)}</span><span class="tag llm">规则生成中文介绍</span></div>
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
  const contentMode = "中文介绍：零依赖规则";

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
      <p><strong>来源：</strong><a href="https://github.com/leelige/article">leelige/article</a> 与论文 arXiv 摘要。本页由 <code>scripts/update-paper-recommendations.mjs</code> 自动生成；不会创建 Codex App 对话。中文介绍通过数据库术语、方法线索和实验指标规则生成，不调用模型 API，也不安装额外依赖；仅在全部内容通过中文与去重检查后发布。每个方向最多保留 5 篇，方向内部按发布时间倒序排列。</p>
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

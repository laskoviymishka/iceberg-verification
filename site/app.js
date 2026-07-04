// Licensed to the Apache Software Foundation (ASF) under the Apache License 2.0.
"use strict";

const STATUS = {
  pass: { label: "pass", cls: "s-pass" },
  fail: { label: "fail", cls: "s-fail" },
  "declared-gap": { label: "gap", cls: "s-declared-gap" },
  "undeclared-gap": { label: "drift", cls: "s-undeclared-gap" },
  oracle: { label: "oracle", cls: "s-oracle" },
  reject: { label: "reject", cls: "s-reject" },
  error: { label: "error", cls: "s-error" },
  "bad-fixture": { label: "bad spec", cls: "s-bad-fixture" },
};

const LEGEND = [
  ["pass", "matches the golden"],
  ["declared-gap", "unsupported + declared in supports.yaml"],
  ["undeclared-gap", "exit 4 for a feature the runner CLAIMED — drift"],
  ["oracle", "executed, no golden (invariant mode)"],
  ["fail", "ran but output differs from golden"],
  ["error", "crashed / unexpected"],
  ["reject", "input correctly rejected"],
  ["bad-fixture", "spec invalid"],
];

let REPORT = null;

async function boot() {
  try {
    const res = await fetch("report.json");
    REPORT = await res.json();
  } catch (e) {
    document.querySelector("main").innerHTML =
      `<p style="color:var(--fail)">could not load report.json — run the orchestrator first.</p>`;
    return;
  }
  document.getElementById("generated-at").textContent = REPORT.generated_at || "—";
  renderLegend();
  renderSummary();
  renderMatrix();
  renderCounts();
  window.addEventListener("hashchange", route);
  route();
  wireTheme();
}

function cellFor(fixture, runner) {
  return REPORT.cells.find((c) => c.fixture === fixture && c.runner === runner);
}

function renderLegend() {
  document.getElementById("legend").innerHTML = LEGEND.map(
    ([k, desc]) =>
      `<span class="legend-item"><span class="swatch ${STATUS[k].cls}" style="background:currentColor"></span>${STATUS[k].label} — ${desc}</span>`
  ).join("");
}

function renderSummary() {
  const counts = {};
  for (const c of REPORT.cells) counts[c.status] = (counts[c.status] || 0) + 1;
  const order = ["pass", "declared-gap", "oracle", "undeclared-gap", "fail", "error"];
  const tiles = order
    .filter((k) => counts[k])
    .map(
      (k) =>
        `<div class="tile"><div class="tile-num ${STATUS[k].cls}">${counts[k]}</div><div class="tile-label">${STATUS[k].label}</div></div>`
    );
  tiles.unshift(
    `<div class="tile"><div class="tile-num" style="color:var(--accent)">${REPORT.fixtures.length}×${REPORT.runners.length}</div><div class="tile-label">fixtures × impls</div></div>`
  );
  document.getElementById("summary").innerHTML = tiles.join("");
}

function renderMatrix() {
  const runners = REPORT.runners;
  let head = `<thead><tr><th>fixture</th>`;
  for (const r of runners)
    head += `<th class="runner-col"><span class="runner-name">${r.name}</span><span class="runner-ver">${r.version || ""}</span></th>`;
  head += `</tr></thead>`;

  let body = "<tbody>";
  for (const fx of REPORT.fixtures) {
    body += `<tr>`;
    const tags = [
      fx.source === "artifact" ? "read" : "write",
      "v" + (fx.format_version ?? "?"),
      fx.has_golden ? "golden" : "oracle",
    ];
    body += `<td class="fixture-cell" onclick="location.hash='#/fixture/${fx.id}'">
      <div class="fixture-id">${fx.id}</div>
      <div class="fixture-tags">${tags.map((t) => `<span class="fx-tag">${t}</span>`).join("")}</div>
    </td>`;
    for (const r of runners) {
      const c = cellFor(fx.id, r.name);
      const s = STATUS[c.status] || STATUS.error;
      body += `<td class="cell">
        <button class="${s.cls}" onclick="location.hash='#/fixture/${fx.id}'" title="${c.status}">
          <span class="dot" style="background:currentColor"></span>
          <span class="cell-status">${s.label}</span>
        </button>
      </td>`;
    }
    body += `</tr>`;
  }
  body += "</tbody>";
  document.getElementById("matrix").innerHTML = head + body;
}

function renderCounts() {
  const pass = REPORT.cells.filter((c) => c.status === "pass").length;
  document.getElementById("counts").textContent =
    `${pass}/${REPORT.cells.length} cells green · report ${REPORT.schema}`;
}

// ---- routing ----
function route() {
  const m = location.hash.match(/^#\/fixture\/(.+)$/);
  const matrix = document.getElementById("matrix-view");
  const detail = document.getElementById("fixture-view");
  if (m) {
    const fx = REPORT.fixtures.find((f) => f.id === decodeURIComponent(m[1]));
    if (fx) {
      matrix.hidden = true;
      detail.hidden = false;
      renderFixture(fx);
      window.scrollTo(0, 0);
      return;
    }
  }
  matrix.hidden = false;
  detail.hidden = true;
}

function renderFixture(fx) {
  const badges = [
    `<span class="badge ${fx.source === "artifact" ? "" : "hot"}">${fx.source === "artifact" ? "READ" : "WRITE"}</span>`,
    `<span class="badge">format v${fx.format_version ?? "?"}</span>`,
    `<span class="badge">${fx.has_golden ? "authored golden" : "oracle mode"}</span>`,
  ].join("");

  const timeline = fx.ops.map(opNode).join("");

  const results = REPORT.runners
    .map((r) => {
      const c = cellFor(fx.id, r.name);
      const s = STATUS[c.status] || STATUS.error;
      return `<div>
        <div class="result-row ${s.cls}" style="border-left-color:currentColor" onclick="this.nextElementSibling.hidden=!this.nextElementSibling.hidden">
          <span class="dot" style="background:currentColor"></span>
          <span class="r-name" style="color:var(--ink)">${r.name}</span>
          <span class="r-status">${s.label}</span>
        </div>
        <div class="result-detail" hidden>${renderDetail(c)}</div>
      </div>`;
    })
    .join("");

  const golden = fx.golden
    ? `<div class="panel"><h3>golden — canonical logical result</h3><pre class="code">${esc(JSON.stringify(fx.golden, null, 2))}</pre></div>`
    : `<div class="panel"><h3>oracle mode</h3><p style="color:var(--ink-dim)">No authored golden. Correctness is a metamorphic invariant the orchestrator checks (future work) — e.g. compaction is a logical no-op on the row multiset.</p></div>`;

  document.getElementById("fixture-view").innerHTML = `
    <button class="back" onclick="location.hash=''">← matrix</button>
    <div class="fixture-head">
      <h2>${fx.id}</h2>
      <div class="badges">${badges}</div>
    </div>
    <div class="detail-grid">
      <div>
        <div class="panel"><h3>operation log</h3><ul class="timeline">${timeline}</ul></div>
        <div class="panel" style="margin-top:28px"><h3>results</h3><div class="results">${results}</div></div>
      </div>
      <div>${golden}
        <div class="panel" style="margin-top:28px"><h3>fixture source (l-log)</h3><pre class="code">${esc(fx.yaml)}</pre></div>
      </div>
    </div>`;
}

function opNode(op) {
  const mutating = ["append", "delete", "overwrite", "rewrite", "evolve-schema", "evolve-spec"];
  const isMut = mutating.includes(op.op);
  const cls = isMut ? "mut" : op.op === "observe" ? "obs" : "";
  const glyph = isMut ? "◆" : op.op === "observe" ? "◎" : "·";
  let detail = "";
  if (op.op === "observe") {
    detail = `read at <code>${op.at || "latest"}</code>` + (op.bind ? ` · bind <code>${op.bind}</code>` : "");
  } else if (op.op === "delete") {
    detail = `<code>${op.strategy || "merge-on-read"}</code> · <code>${op.kind || "position"}</code>`;
  }
  return `<li class="op ${cls}">
    <span class="op-node">${glyph}</span>
    <div class="op-name">${op.op}</div>
    ${detail ? `<div class="op-detail">${detail}</div>` : ""}
  </li>`;
}

function renderDetail(c) {
  const d = c.detail || {};
  if (c.status === "pass") return "output matches the golden — logical row-set, snapshots, and schema all agree.";
  if (c.status === "declared-gap")
    return `unsupported feature(s): <span class="path">${(d.missing || []).join(", ")}</span>\ndeclared in supports.yaml → honest matrix gap.\n\n${esc(d.stderr || "")}`;
  if (c.status === "undeclared-gap")
    return `DRIFT: the runner exited "unsupported" but its supports.yaml CLAIMS this.\nrequired: <span class="path">${(d.required || []).join(", ")}</span>\n\n${esc(d.stderr || "")}`;
  if (c.status === "oracle") return `executed; emitted ${d.observations} observation(s). Invariant check is orchestrator/future work.`;
  if (c.status === "error") return `crashed or exited unexpectedly (exit ${d.exit ?? "?"}).\n\n${esc(d.stderr || "")}`;
  if (c.status === "bad-fixture") return `spec rejected as invalid.\n\n${esc(d.stderr || "")}`;
  if (c.status === "reject") return `input correctly rejected.\n\n${esc(d.stderr || "")}`;
  if (c.status === "fail") {
    const diffs = (d.diffs || [])
      .map(
        (df) =>
          `<span class="path">${df.path}</span>\n  <span class="del">- want</span> ${esc(JSON.stringify(df.want))}\n  <span class="add">+ got </span> ${esc(JSON.stringify(df.got))}`
      )
      .join("\n\n");
    return `output differs from golden:\n\n${diffs}`;
  }
  return esc(JSON.stringify(d, null, 2));
}

function esc(s) {
  return String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}

function wireTheme() {
  const btn = document.getElementById("theme-toggle");
  btn.addEventListener("click", () => {
    const root = document.documentElement;
    const next = root.getAttribute("data-theme") === "light" ? "dark" : "light";
    root.setAttribute("data-theme", next);
    btn.textContent = next === "light" ? "☀" : "☾";
  });
}

boot();

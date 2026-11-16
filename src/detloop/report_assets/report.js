/* detloop offline report. All trace-derived text is inserted with textContent
   or SVG text nodes; nothing from the data is ever parsed as HTML. */
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("detloop-data").textContent);
  var SVGNS = "http://www.w3.org/2000/svg";
  var root = document.getElementById("app");

  function el(tag, attrs, text) {
    var e = document.createElement(tag);
    if (attrs) for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, attrs[k]);
    if (text !== undefined && text !== null) e.textContent = String(text);
    return e;
  }
  function sv(tag, attrs, text) {
    var e = document.createElementNS(SVGNS, tag);
    if (attrs) for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, String(attrs[k]));
    if (text !== undefined && text !== null) e.textContent = String(text);
    return e;
  }
  function panel(title) {
    var s = el("section", { "class": "panel" });
    s.appendChild(el("h2", null, title));
    root.appendChild(s);
    return s;
  }
  function secs(ns) { return (ns / 1e9).toFixed(3) + "s"; }
  function baseNode(n) { return String(n || "").split("#")[0]; }
  function stat(grid, value, label) {
    var d = el("div", { "class": "stat" });
    d.appendChild(el("b", null, value));
    d.appendChild(el("span", null, label));
    grid.appendChild(d);
  }

  if (DATA.kind === "exploration") { renderExploration(DATA); return; }
  renderRun(DATA);

  // ------------------------------------------------------------------ run
  function renderRun(d) {
    var m = d.manifest || {};
    var events = d.events || [];
    var outcome = m.outcome || {};
    var head = document.getElementById("head");
    var badge = el("span", { "class": "badge " + (outcome.status || "fail") }, outcome.status || "trace");
    head.appendChild(badge);
    head.appendChild(document.createTextNode(" "));
    head.appendChild(el("code", null, outcome.signature || ""));
    if (outcome.message) head.appendChild(el("div", { "class": "message" }, outcome.message));

    var sum = panel("Summary");
    var g = el("div", { "class": "grid" });
    var endT = events.length ? events[events.length - 1].t : 0;
    stat(g, m.seed !== undefined ? m.seed : "?", "seed");
    stat(g, secs(endT), "virtual time");
    stat(g, events.length, "trace events" + (d.truncated ? " (truncated)" : ""));
    var st = m.stats || {};
    if (st.steps !== undefined) stat(g, st.steps, "loop steps");
    stat(g, (m.faults || []).length, "scheduled faults");
    var ops = events.filter(function (e) { return e.k === "invoke"; }).length;
    stat(g, ops, "operations");
    stat(g, m.scheduler || "?", "scheduler");
    sum.appendChild(g);
    if (m.target) sum.appendChild(el("p", { "class": "sub" }, "test: " + m.target.location + "::" + m.target.test));
    if (d.replay && d.replay.length) {
      sum.appendChild(el("h2", null, "Replay"));
      d.replay.forEach(function (c) { sum.appendChild(el("pre", { "class": "cmd" }, c)); });
    }
    if (outcome.details && outcome.details.length) {
      sum.appendChild(el("h2", null, "Blocked tasks"));
      var ul = el("ul", { "class": "plain" });
      outcome.details.forEach(function (x) { ul.appendChild(el("li", { "class": "mono" }, x)); });
      sum.appendChild(ul);
    }
    if ((m.faults || []).length) {
      var fp = panel("Fault schedule");
      var ful = el("ul", { "class": "plain" });
      m.faults.forEach(function (f) {
        var li = el("li");
        li.appendChild(el("code", null, f.id));
        li.appendChild(document.createTextNode(" t=" + secs(f.at) + " " + f.kind + " " + JSON.stringify(f.params) + (f.duration ? " for " + secs(f.duration) : "")));
        var b = el("button", { "class": "link" }, "events");
        b.addEventListener("click", function () { table.filterBy("fault", f.id); });
        li.appendChild(b);
        ful.appendChild(li);
      });
      fp.appendChild(ful);
    }
    var table = null;
    renderTimeline(events, endT, d.nodes || [], function (id) { table.focus(id); });
    renderHistory(events, endT, function (id) { table.focus(id); });
    var checks = events.filter(function (e) { return e.k === "check" || e.k === "invariant_failed" || e.k === "loop_error" || e.k === "nondeterminism" || e.k === "node_error"; });
    if (checks.length) {
      var cp = panel("Checks and errors");
      var cul = el("ul", { "class": "plain" });
      checks.forEach(function (e) {
        var li = el("li");
        var b = el("button", { "class": "link" }, "#" + e.i);
        b.addEventListener("click", function () { table.focus(e.i); });
        li.appendChild(b);
        li.appendChild(document.createTextNode(" " + secs(e.t) + " " + e.k + ": " + (e.status || e.name || e.message || "") + " " + (e.reason || e.message || e.detail || "")));
        cul.appendChild(li);
      });
      cp.appendChild(cul);
    }
    table = renderEvents(events);
  }

  function lanesFrom(events, nodes) {
    var lanes = ["control"];
    nodes.forEach(function (n) { if (lanes.indexOf(n) < 0) lanes.push(n); });
    events.forEach(function (e) {
      var n = baseNode(e.node);
      if (n && lanes.indexOf(n) < 0) lanes.push(n);
    });
    return lanes;
  }

  function renderTimeline(events, endT, nodes, focus) {
    var p = panel("Node timeline");
    var lanes = lanesFrom(events, nodes);
    var W = 1100, L = 90, R = 16, H = 30, top = 22;
    var span = Math.max(endT, 1);
    function x(t) { return L + (W - L - R) * (t / span); }
    var svg = sv("svg", { viewBox: "0 0 " + W + " " + (top + lanes.length * H + 24), width: "100%", role: "img", "aria-label": "node timeline" });
    lanes.forEach(function (n, i) {
      var y = top + i * H;
      svg.appendChild(sv("rect", { x: L, y: y + 4, width: W - L - R, height: H - 8, "class": "lane" }));
      svg.appendChild(sv("text", { x: 4, y: y + H / 2 + 4 }, n));
    });
    function laneY(n) { var i = lanes.indexOf(baseNode(n)); return i < 0 ? null : top + i * H; }
    // up intervals
    var open = {};
    events.forEach(function (e) {
      if (e.k === "node_start") open[baseNode(e.node)] = e.t;
      if (e.k === "node_crash") {
        var n = baseNode(e.node), y = laneY(n);
        if (open[n] !== undefined && y !== null) svg.appendChild(sv("rect", { x: x(open[n]), y: y + 8, width: Math.max(1, x(e.t) - x(open[n])), height: H - 16, "class": "up" }));
        delete open[n];
        if (y !== null) {
          var c = sv("line", { x1: x(e.t), x2: x(e.t), y1: y + 3, y2: y + H - 3, "class": "crash clickable" });
          c.appendChild(sv("title", null, "crash " + e.node + " at " + secs(e.t)));
          c.addEventListener("click", function () { focus(e.i); });
          svg.appendChild(c);
        }
      }
    });
    Object.keys(open).forEach(function (n) {
      var y = laneY(n);
      if (y !== null) svg.appendChild(sv("rect", { x: x(open[n]), y: y + 8, width: Math.max(1, x(endT) - x(open[n])), height: H - 16, "class": "up" }));
    });
    // partitions from scheduled faults
    events.forEach(function (e) {
      if (e.k === "fault" && e.kind === "partition") {
        var names = e.params.oneway ? [e.params.a] : [].concat(e.params.groups[0]);
        names.forEach(function (n) {
          var y = laneY(n);
          if (y === null) return;
          var r = sv("rect", { x: x(e.t), y: y + 2, width: Math.max(2, x(e.t + (e.duration || 0)) - x(e.t)), height: H - 4, "class": "part clickable" });
          r.appendChild(sv("title", null, e.fault + " partition " + JSON.stringify(e.params) + " from " + secs(e.t)));
          r.addEventListener("click", function () { focus(e.i); });
          svg.appendChild(r);
        });
      }
      if (e.k === "fault") {
        var tri = sv("path", { d: "M" + x(e.t) + " 4 l5 9 l-10 0 z", "class": "fault clickable" });
        tri.appendChild(sv("title", null, e.fault + " " + e.kind + " at " + secs(e.t)));
        tri.addEventListener("click", function () { focus(e.i); });
        svg.appendChild(tri);
      }
      if (e.k === "note" && e.node) {
        var ny = laneY(e.node);
        if (ny !== null) {
          var dm = sv("circle", { cx: x(e.t), cy: ny + H / 2, r: 3.5, "class": "note clickable" });
          dm.appendChild(sv("title", null, (e.note || "note") + " " + JSON.stringify(e) ));
          dm.addEventListener("click", function () { focus(e.i); });
          svg.appendChild(dm);
        }
      }
      if (e.k === "invariant_failed" || (e.k === "end" && e.status && e.status !== "pass")) {
        var l = sv("line", { x1: x(e.t), x2: x(e.t), y1: top - 4, y2: top + lanes.length * H, "class": "inv clickable" });
        l.appendChild(sv("title", null, e.k + " " + (e.name || e.signature || "")));
        l.addEventListener("click", function () { focus(e.i); });
        svg.appendChild(l);
      }
    });
    var axisY = top + lanes.length * H + 14;
    for (var k = 0; k <= 5; k++) {
      var t = span * k / 5;
      svg.appendChild(sv("text", { x: x(t) - 12, y: axisY, "class": "muted" }, secs(t)));
    }
    var box = el("div", { "class": "scroll" });
    box.appendChild(svg);
    p.appendChild(box);
    var lg = el("div", { "class": "legend" });
    [["var(--up)", "node up"], ["var(--part)", "partitioned"], ["var(--fail)", "crash / failure"], ["var(--info)", "fault"], ["var(--accent)", "note"]].forEach(function (it) {
      var s = el("span"); var i = el("i"); i.style.background = it[0]; s.appendChild(i); s.appendChild(document.createTextNode(it[1])); lg.appendChild(s);
    });
    p.appendChild(lg);
  }

  function renderHistory(events, endT, focus) {
    var inv = {}, rows = [], clients = [];
    events.forEach(function (e) {
      if (e.k === "invoke") { inv[e.op] = e; if (clients.indexOf(e.client) < 0) clients.push(e.client); }
      if (e.k === "complete" && inv[e.op]) { rows.push([inv[e.op], e]); delete inv[e.op]; }
    });
    Object.keys(inv).forEach(function (k) { rows.push([inv[k], null]); });
    if (!rows.length) return;
    var p = panel("Operation history");
    var W = 1100, L = 90, R = 16, H = 22, top = 8;
    var span = Math.max(endT, 1);
    function x(t) { return L + (W - L - R) * (t / span); }
    var svg = sv("svg", { viewBox: "0 0 " + W + " " + (top + clients.length * H + 8), width: "100%", role: "img", "aria-label": "operation history" });
    clients.forEach(function (c, i) {
      svg.appendChild(sv("rect", { x: L, y: top + i * H + 2, width: W - L - R, height: H - 4, "class": "lane" }));
      svg.appendChild(sv("text", { x: 4, y: top + i * H + H / 2 + 4 }, c));
    });
    rows.forEach(function (r) {
      var a = r[0], b = r[1];
      var status = b ? b.status : "pending";
      var y = top + clients.indexOf(a.client) * H + 5;
      var x2 = b ? x(b.t) : x(endT);
      var rect = sv("rect", { x: x(a.t), y: y, width: Math.max(2, x2 - x(a.t)), height: H - 10, "class": "op-" + status + " clickable" });
      var label = "#" + a.op + " " + a.f + (a.key !== undefined ? "[" + JSON.stringify(a.key) + "]" : "") + (a.value !== undefined ? "(" + JSON.stringify(a.value) + ")" : "") + " -> " + status + (b && b.output !== undefined ? " " + JSON.stringify(b.output) : "") + (b && b.error ? " " + b.error : "");
      rect.appendChild(sv("title", null, label));
      rect.addEventListener("click", function () { focus(a.i); });
      svg.appendChild(rect);
    });
    var box = el("div", { "class": "scroll" });
    box.appendChild(svg);
    p.appendChild(box);
    var lg = el("div", { "class": "legend" });
    [["var(--ok)", "ok"], ["var(--info)", "info (indeterminate)"], ["var(--pend)", "fail (did not happen)"]].forEach(function (it) {
      var s = el("span"); var i = el("i"); i.style.background = it[0]; s.appendChild(i); s.appendChild(document.createTextNode(it[1])); lg.appendChild(s);
    });
    p.appendChild(lg);
  }

  function renderEvents(events) {
    var p = panel("Events");
    var controls = el("div", { "class": "controls" });
    var q = el("input", { type: "search", placeholder: "filter text (e.g. n1, c12, op 7, partition)", "aria-label": "filter events" });
    var kinds = {};
    events.forEach(function (e) { kinds[e.k] = (kinds[e.k] || 0) + 1; });
    var sel = el("select", { "aria-label": "event kind" });
    sel.appendChild(el("option", { value: "" }, "all kinds"));
    Object.keys(kinds).sort().forEach(function (k) { sel.appendChild(el("option", { value: k }, k + " (" + kinds[k] + ")")); });
    var clear = el("button", null, "clear");
    controls.appendChild(q); controls.appendChild(sel); controls.appendChild(clear);
    p.appendChild(controls);
    var tbl = el("table");
    var thead = el("thead"), tr = el("tr");
    ["#", "time", "kind", "node", "details"].forEach(function (h) { tr.appendChild(el("th", null, h)); });
    thead.appendChild(tr); tbl.appendChild(thead);
    var tbody = el("tbody"); tbl.appendChild(tbody);
    var box = el("div", { "class": "scroll" }); box.appendChild(tbl); p.appendChild(box);
    var pager = el("div", { "class": "pager" });
    var prev = el("button", null, "prev"), next = el("button", null, "next"), info = el("span");
    pager.appendChild(prev); pager.appendChild(info); pager.appendChild(next); p.appendChild(pager);
    var PAGE = 300, page = 0, rel = null, highlight = -1, filtered = events;

    function matches(e) {
      if (sel.value && e.k !== sel.value) return false;
      if (rel && String(e[rel[0]]) !== String(rel[1])) return false;
      var s = q.value.trim().toLowerCase();
      if (!s) return true;
      return JSON.stringify(e).toLowerCase().indexOf(s) >= 0;
    }
    function relButton(td, key, value, label) {
      var b = el("button", { "class": "link" }, label);
      b.addEventListener("click", function () { api.filterBy(key, value); });
      td.appendChild(b);
    }
    function draw() {
      filtered = events.filter(matches);
      var pages = Math.max(1, Math.ceil(filtered.length / PAGE));
      if (page >= pages) page = pages - 1;
      while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
      filtered.slice(page * PAGE, (page + 1) * PAGE).forEach(function (e) {
        var r = el("tr", { id: "ev-" + e.i });
        if (e.i === highlight) r.className = "hl";
        r.appendChild(el("td", { "class": "mono" }, e.i));
        r.appendChild(el("td", { "class": "mono" }, secs(e.t)));
        r.appendChild(el("td", null, e.k));
        r.appendChild(el("td", null, e.node || ""));
        var td = el("td", { "class": "fields mono" });
        var rest = {};
        Object.keys(e).forEach(function (k) { if (["i", "t", "k", "node"].indexOf(k) < 0) rest[k] = e[k]; });
        td.appendChild(document.createTextNode(JSON.stringify(rest) + " "));
        if (e.conn) relButton(td, "conn", e.conn, "conn " + e.conn);
        if (e.op !== undefined) relButton(td, "op", e.op, "op " + e.op);
        if (e.fault) relButton(td, "fault", e.fault, e.fault);
        if (e.msg !== undefined) relButton(td, "msg", e.msg, "msg " + e.msg);
        r.appendChild(td);
        tbody.appendChild(r);
      });
      info.textContent = filtered.length + " events" + (rel ? " related to " + rel[0] + " " + rel[1] : "") + " - page " + (page + 1) + "/" + pages;
    }
    var api = {
      filterBy: function (key, value) { rel = [key, value]; page = 0; draw(); p.scrollIntoView(); },
      focus: function (id) {
        rel = null; q.value = ""; sel.value = ""; highlight = id;
        filtered = events.filter(matches);
        var idx = 0;
        for (var j = 0; j < filtered.length; j++) if (filtered[j].i === id) { idx = j; break; }
        page = Math.floor(idx / PAGE); draw();
        var row = document.getElementById("ev-" + id);
        if (row) row.scrollIntoView({ block: "center" });
      }
    };
    q.addEventListener("input", function () { page = 0; draw(); });
    sel.addEventListener("change", function () { page = 0; draw(); });
    clear.addEventListener("click", function () { rel = null; q.value = ""; sel.value = ""; highlight = -1; page = 0; draw(); });
    prev.addEventListener("click", function () { if (page > 0) { page--; draw(); } });
    next.addEventListener("click", function () { page++; draw(); });
    draw();
    return api;
  }

  // ------------------------------------------------------------ exploration
  function renderExploration(d) {
    var head = document.getElementById("head");
    head.appendChild(el("span", { "class": "sub" }, "results database: " + d.path));
    (d.explorations || []).forEach(function (ex) {
      var p = panel("#" + ex.id + " " + ex.test + " (" + ex.status + ")");
      var counts = { pass: 0, fail: 0, incomplete: 0, error: 0 };
      var sigs = {};
      ex.runs.forEach(function (r) { counts[r.status] = (counts[r.status] || 0) + 1; if (r.status !== "pass") { (sigs[r.signature] = sigs[r.signature] || []).push(r.seed); } });
      var g = el("div", { "class": "grid" });
      stat(g, ex.runs.length + "/" + ex.seed_count, "seeds completed");
      stat(g, counts.pass, "passed"); stat(g, counts.fail, "failed"); stat(g, counts.incomplete, "incomplete"); stat(g, counts.error, "errors");
      stat(g, ex.jobs, "jobs"); stat(g, ex.python, "python"); stat(g, ex.detloop_version, "detloop");
      p.appendChild(g);
      p.appendChild(el("p", { "class": "sub" }, ex.target + " - seeds " + ex.seed_start + ".." + (ex.seed_start + ex.seed_count - 1) + " - started " + ex.started_at + " - config " + ex.config_fingerprint));
      var keys = Object.keys(sigs);
      if (keys.length) {
        p.appendChild(el("h2", null, "Failure signatures"));
        var ul = el("ul", { "class": "plain" });
        keys.sort().forEach(function (k) { ul.appendChild(el("li", { "class": "mono" }, k + ": " + sigs[k].length + " seed(s): " + sigs[k].slice(0, 20).join(", "))); });
        p.appendChild(ul);
        var tbl = el("table"), tr = el("tr");
        ["seed", "status", "signature", "message", "virtual time", "steps", "artifact"].forEach(function (h) { tr.appendChild(el("th", null, h)); });
        var th = el("thead"); th.appendChild(tr); tbl.appendChild(th);
        var tb = el("tbody");
        ex.runs.filter(function (r) { return r.status !== "pass"; }).slice(0, 500).forEach(function (r) {
          var row = el("tr");
          [r.seed, r.status, r.signature, r.message, secs(r.virtual_ns), r.steps, r.artifact || ""].forEach(function (v) { row.appendChild(el("td", { "class": "mono" }, v)); });
          tb.appendChild(row);
        });
        tbl.appendChild(tb);
        var box = el("div", { "class": "scroll" }); box.appendChild(tbl); p.appendChild(box);
      }
    });
  }
})();

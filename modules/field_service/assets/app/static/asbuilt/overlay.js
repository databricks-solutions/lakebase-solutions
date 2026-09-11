/* =====================================================================
   AsBuilt Overlay engine — layered on the vendored IDEA base.

   Reads asbuilt_state.json and:
     1. dims boxes with no discovered resource, illuminates the rest
     2. draws two line tiers between illuminated boxes:
          core     — solid, animated (the solution's data/AI spine)
          feature  — dashed, muted (a capability a flow relies on)
     3. re-applies on resize / zoom-pan / theme / industry-cloud switch
        (IDEA re-renders boxes on those, which would wipe our classes)

   Coordinate model: our SVG mounts INSIDE #board, which is scaled by an
   ancestor (#fit-in) transform. Box rects are read via getBoundingClientRect
   and converted to board-local, unscaled coords by dividing out that scale,
   so lines stay aligned at any zoom.

   Public API: window.AsBuilt.refresh()  — re-fetch state and redraw (used by
   the /asbuilt/refresh live-refresh endpoint after regenerating state).
   ===================================================================== */
(function () {
  "use strict";

  // The AsBuilt page is a focused review view, not the interactive explorer, so
  // suppress the IDEA onboarding tour (its maybeAuto() skips when this flag is set).
  try { localStorage.setItem("dbx-arch-tour-v1", "1"); } catch (_) {}

  var STATE_URLS = ["./asbuilt_state.json", "/static/asbuilt_state.json", "asbuilt_state.json"];
  var SVG_NS = "http://www.w3.org/2000/svg";
  var state = null;
  var svg = null;
  var redrawQueued = false;
  var lastDetailId = null;   // data-id of the box whose detail drawer is open
  var _diag = { draw: 0, flowStart: 0, ticks: 0, pulses: 0, lastOffset: 0 };  // observability

  function boardEl() { return document.getElementById("board"); }

  function currentScale() {
    // Read the scale factor from the nearest ancestor transform (#fit-in).
    var el = document.getElementById("fit-in");
    if (!el) return 1;
    var t = getComputedStyle(el).transform;
    if (!t || t === "none") return 1;
    var m = t.match(/matrix\(([^)]+)\)/);
    if (!m) return 1;
    var a = parseFloat(m[1].split(",")[0]);
    return a && a > 0 ? a : 1;
  }

  // ---- box state application ---------------------------------------------
  function applyStates() {
    if (!state) return;
    var active = new Set(state.active || []);
    var evidence = state.evidence || {};
    document.querySelectorAll("[data-id]").forEach(function (el) {
      var id = el.getAttribute("data-id");
      // Only touch real boxes (buttons); skip label DIVs the base also tags.
      if (el.tagName !== "BUTTON") return;
      el.classList.remove("asbuilt-lit", "asbuilt-dim");
      if (active.has(id)) {
        el.classList.add("asbuilt-lit");
        var ev = evidence[id];
        if (ev && ev.length) el.setAttribute("data-asbuilt-evidence", ev.join(" • "));
      } else {
        el.classList.add("asbuilt-dim");
      }
    });
    document.body.classList.add("asbuilt-on");
  }

  // ---- geometry ----------------------------------------------------------
  function localRect(el, boardRect, scale) {
    var r = el.getBoundingClientRect();
    return {
      x: (r.left - boardRect.left) / scale,
      y: (r.top - boardRect.top) / scale,
      w: r.width / scale,
      h: r.height / scale,
      cx: (r.left - boardRect.left + r.width / 2) / scale,
      cy: (r.top - boardRect.top + r.height / 2) / scale
    };
  }

  // Anchor points on the box border facing the other box, so lines meet edges.
  function anchors(a, b) {
    var dx = b.cx - a.cx, dy = b.cy - a.cy;
    var from, to;
    if (Math.abs(dy) >= Math.abs(dx)) {
      // predominantly vertical
      from = { x: a.cx, y: dy >= 0 ? a.y + a.h : a.y };
      to = { x: b.cx, y: dy >= 0 ? b.y : b.y + b.h };
    } else {
      from = { x: dx >= 0 ? a.x + a.w : a.x, y: a.cy };
      to = { x: dx >= 0 ? b.x : b.x + b.w, y: b.cy };
    }
    return { from: from, to: to, vertical: Math.abs(dy) >= Math.abs(dx) };
  }

  function segHitsRect(x1, y1, x2, y2, r, m) {
    var rx = r.x - m, ry = r.y - m, rx2 = r.x + r.w + m, ry2 = r.y + r.h + m;
    if (rx <= x1 && x1 <= rx2 && ry <= y1 && y1 <= ry2) return true;
    if (rx <= x2 && x2 <= rx2 && ry <= y2 && y2 <= ry2) return true;
    function segInt(ax, ay, bx, by, cx, cy, dx, dy) {
      function cr(ox, oy, px, py, qx, qy) { return (px - ox) * (qy - oy) - (py - oy) * (qx - ox); }
      var d1 = cr(cx, cy, dx, dy, ax, ay), d2 = cr(cx, cy, dx, dy, bx, by);
      var d3 = cr(ax, ay, bx, by, cx, cy), d4 = cr(ax, ay, bx, by, dx, dy);
      return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0));
    }
    var e = [[rx, ry, rx2, ry], [rx, ry2, rx2, ry2], [rx, ry, rx, ry2], [rx2, ry, rx2, ry2]];
    for (var i = 0; i < 4; i++) if (segInt(x1, y1, x2, y2, e[i][0], e[i][1], e[i][2], e[i][3])) return true;
    return false;
  }

  // Sample a quadratic into segments and count obstacle rects any segment hits
  // (mirrors validate_flows.py so the router optimizes the metric we validate).
  function crossings(p1, ctrl, p2, obstacles) {
    var pts = [], STEP = 0.05;
    for (var t = 0; t <= 1.0001; t += STEP) {
      var mt = 1 - t;
      pts.push([mt * mt * p1.x + 2 * mt * t * ctrl.x + t * t * p2.x,
                mt * mt * p1.y + 2 * mt * t * ctrl.y + t * t * p2.y]);
    }
    var hit = 0;
    for (var o = 0; o < obstacles.length; o++) {
      for (var i = 1; i < pts.length; i++) {
        if (segHitsRect(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1], obstacles[o], -8)) { hit++; break; }
      }
    }
    return hit;
  }

  // Count obstacle crossings for a straight polyline (for trunk routes).
  function polyCrossings(pts, obstacles) {
    var hit = 0;
    for (var o = 0; o < obstacles.length; o++) {
      for (var i = 1; i < pts.length; i++) {
        if (segHitsRect(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1], obstacles[o], -8)) { hit++; break; }
      }
    }
    return hit;
  }

  // Build a rounded-corner orthogonal path through a list of [x,y] waypoints.
  function ortho(pts, r) {
    r = r || 9;
    if (pts.length < 2) return "";
    var d = "M" + pts[0][0] + "," + pts[0][1];
    for (var i = 1; i < pts.length - 1; i++) {
      var p = pts[i - 1], c = pts[i], n = pts[i + 1];
      var v1 = [c[0] - p[0], c[1] - p[1]], v2 = [n[0] - c[0], n[1] - c[1]];
      var l1 = Math.hypot(v1[0], v1[1]) || 1, l2 = Math.hypot(v2[0], v2[1]) || 1;
      var rr = Math.min(r, l1 / 2, l2 / 2);
      var a1 = [c[0] - v1[0] / l1 * rr, c[1] - v1[1] / l1 * rr];
      var a2 = [c[0] + v2[0] / l2 * rr, c[1] + v2[1] / l2 * rr];
      d += " L" + a1[0] + "," + a1[1] + " Q" + c[0] + "," + c[1] + " " + a2[0] + "," + a2[1];
    }
    var last = pts[pts.length - 1];
    d += " L" + last[0] + "," + last[1];
    return d;
  }

  // Direct arc: quadratic bowing perpendicular to dodge lit obstacle boxes.
  function directArc(p1, p2, obstacles) {
    var mid = { x: (p1.x + p2.x) / 2, y: (p1.y + p2.y) / 2 };
    var dx = p2.x - p1.x, dy = p2.y - p1.y;
    var len = Math.hypot(dx, dy) || 1;
    var px = -dy / len, py = dx / len;
    var step = Math.max(34, len * 0.22);
    var best = { x: mid.x, y: mid.y }, bestCross = Infinity;
    var offs = [0];
    for (var k = 1; k <= 9; k++) { offs.push(k * step); offs.push(-k * step); }
    for (var oi = 0; oi < offs.length; oi++) {
      var ctrl = { x: mid.x + px * offs[oi], y: mid.y + py * offs[oi] };
      var c = crossings(p1, ctrl, p2, obstacles);
      if (c < bestCross) { bestCross = c; best = ctrl; if (c === 0) break; }
    }
    return { d: "M" + p1.x + "," + p1.y + " Q" + best.x + "," + best.y + " " + p2.x + "," + p2.y,
             cross: bestCross };
  }

  // Trunk route: exit the source VERTICALLY (clearing its own row), run along a
  // clear lane to a vertical trunk placed in the GAP ADJACENT TO THE TARGET (on
  // the source-facing side), then enter the target from that side at its own cy.
  // This reaches a box buried in a stacked column without crossing its siblings.
  // 'far' places the trunk in the outer gutter instead (fallback for odd cases).
  function trunkRoute(a, b, mode, ctx, obstacles) {
    var targetRight = b.cx >= a.cx;
    var gap = 14;
    var trunkX, bx;
    if (mode === "far") {
      trunkX = targetRight ? ctx.platformR + 16 : ctx.platformL - 16;
      bx = targetRight ? b.x + b.w : b.x;
    } else {
      trunkX = targetRight ? b.x - gap : b.x + b.w + gap;  // gap beside the target
      bx = targetRight ? b.x : b.x + b.w;                  // enter from source-facing side
    }
    var up = b.cy < a.cy;
    var srcExitY = up ? a.y : a.y + a.h;
    // Try several exit-lane y positions; pick the clearest horizontal run.
    var lanes = up
      ? [a.y - 12, Math.min(a.y, b.y) - 12]
      : [a.y + a.h + 12, Math.max(a.y + a.h, b.y + b.h) + 12];
    var best = null, bestCross = Infinity;
    for (var i = 0; i < lanes.length; i++) {
      var pts = [[a.cx, srcExitY], [a.cx, lanes[i]], [trunkX, lanes[i]], [trunkX, b.cy], [bx, b.cy]];
      var c = polyCrossings(pts, obstacles);
      if (c < bestCross) { bestCross = c; best = pts; if (c === 0) break; }
    }
    return { d: ortho(best), cross: bestCross, pts: best };
  }

  // Choose the best route for an edge given an optional hint.
  function routeEdge(a, b, hint, ctx, obstacles) {
    var an = anchors(a, b);
    var sameRow = Math.abs(a.cy - b.cy) < Math.max(a.h, b.h) * 0.9;
    var candidates = [];
    if (hint === "direct") {
      candidates.push(directArc(an.from, an.to, obstacles));
    } else if (sameRow) {
      // same row: a short arc is cleanest, but a neighbour may sit directly
      // between endpoints — fall back to trunk routes if the arc can't dodge.
      candidates.push(directArc(an.from, an.to, obstacles));
      candidates.push(trunkRoute(a, b, "near", ctx, obstacles));
      candidates.push(trunkRoute(a, b, "far", ctx, obstacles));
    } else {
      // cross-band: prefer the target-adjacent trunk (clean for stacked columns),
      // then the outer-gutter trunk, then a direct arc. Ties keep push order.
      candidates.push(trunkRoute(a, b, "near", ctx, obstacles));
      candidates.push(trunkRoute(a, b, "far", ctx, obstacles));
      candidates.push(directArc(an.from, an.to, obstacles));
    }
    candidates.sort(function (x, y) { return x.cross - y.cross; });  // stable: ties keep push order
    return candidates[0];
  }

  function mkPath(d, cls) {
    var p = document.createElementNS(SVG_NS, "path");
    p.setAttribute("d", d);
    p.setAttribute("class", cls);
    return p;
  }

  // ---- draw --------------------------------------------------------------
  function ensureSvg(board) {
    if (svg && svg.parentNode === board) return svg;
    svg = document.createElementNS(SVG_NS, "svg");
    svg.setAttribute("class", "asbuilt-svg");
    // arrowhead marker for core edges
    var defs = document.createElementNS(SVG_NS, "defs");
    defs.innerHTML =
      '<marker id="ab-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" ' +
      'markerHeight="6" orient="auto-start-reverse">' +
      '<path d="M0,0 L10,5 L0,10 z" fill="var(--ab-core)"/></marker>';
    svg.appendChild(defs);
    board.appendChild(svg);
    return svg;
  }

  function boxById(id) {
    return document.querySelector('button[data-id="' + id + '"]');
  }

  function drawEdges() {
    var board = boardEl();
    if (!board || !state) return;
    _diag.draw++;
    var s = ensureSvg(board);
    // clear previous paths/labels (keep <defs>)
    [].slice.call(s.querySelectorAll(".asbuilt-core,.asbuilt-core-pulse,.asbuilt-feature,.asbuilt-elabel,.asbuilt-elabel-bg")).forEach(function (n) { n.remove(); });

    s.setAttribute("width", board.offsetWidth);
    s.setAttribute("height", board.offsetHeight);
    s.setAttribute("viewBox", "0 0 " + board.offsetWidth + " " + board.offsetHeight);

    var boardRect = board.getBoundingClientRect();
    var scale = currentScale();
    var rectCache = {};
    function rectFor(id) {
      if (rectCache[id] !== undefined) return rectCache[id];
      var el = boxById(id);
      rectCache[id] = el ? localRect(el, boardRect, scale) : null;
      return rectCache[id];
    }

    // Obstacles = all lit boxes; each edge excludes its own two endpoints.
    var litRects = {};
    (state.active || []).forEach(function (id) { var r = rectFor(id); if (r) litRects[id] = r; });
    function obstaclesFor(fromId, toId) {
      var out = [];
      for (var id in litRects) {
        if (id === fromId || id === toId) continue;
        out.push(litRects[id]);
      }
      return out;
    }

    // Platform box bounds (bands a1..a42) define the side gutters trunk routes use.
    var pL = Infinity, pR = -Infinity;
    for (var pid in litRects) {
      var n = parseInt(pid.slice(1), 10);
      if (n >= 1 && n <= 42) { pL = Math.min(pL, litRects[pid].x); pR = Math.max(pR, litRects[pid].x + litRects[pid].w); }
    }
    if (!isFinite(pL)) { pL = 0; pR = board.offsetWidth; }
    var ctx = { platformL: pL, platformR: pR };

    function tag(path, e) {
      path.setAttribute("data-from", e.from);
      path.setAttribute("data-to", e.to);
      if (e.label) path.setAttribute("data-label", e.label);
      return path;
    }

    // feature first (under core)
    (state.featureEdges || []).forEach(function (e) {
      var a = rectFor(e.from), b = rectFor(e.to);
      if (!a || !b) return;
      var r = routeEdge(a, b, e.route, ctx, obstaclesFor(e.from, e.to));
      s.appendChild(tag(mkPath(r.d, "asbuilt-feature"), e));
    });

    (state.coreEdges || []).forEach(function (e) {
      var a = rectFor(e.from), b = rectFor(e.to);
      if (!a || !b) return;
      var r = routeEdge(a, b, e.route, ctx, obstaclesFor(e.from, e.to));
      // solid pipe (the path) + animated pulse travelling along it (flow)
      var base = tag(mkPath(r.d, "asbuilt-core"), e);
      base.setAttribute("marker-end", "url(#ab-arrow)");
      s.appendChild(base);
      // Pulse styled INLINE so no stale/cached overlay.css (old reduce-motion
      // display:none or CSS @keyframes) can hide or fight the JS-driven motion.
      var pulse = tag(mkPath(r.d, "asbuilt-core-pulse"), e);
      pulse.style.cssText = "fill:none;stroke:#eafff5;stroke-width:3px;opacity:.95;animation:none";
      pulse.setAttribute("stroke-dasharray", "12 16");
      pulse.setAttribute("stroke-dashoffset", flowOffset);   // continue from current flow position (no reset flash on redraw)
      s.appendChild(pulse);
    });
  }

  function escAttr(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  // Slim app nav bar at the top of the full-page AsBuilt (Databricks Apps set
  // X-Frame-Options: DENY, so the map can't live inside the app's own chrome).
  // Reads ?home (app root / "Back" target), ?app (app name), and ?nav (JSON
  // [{l,h},...] quick-links the host app passes). Pushes IDEA's sticky header +
  // content down so nothing is covered.
  function renderTopBar() {
    if (document.querySelector(".asbuilt-navbar")) return;
    var home = null, app = null, nav = [];
    try {
      var q = new URLSearchParams(location.search);
      home = q.get("home"); app = q.get("app");
      var n = q.get("nav"); if (n) nav = JSON.parse(n);
    } catch (_) {}
    if (!home && !(nav && nav.length)) return;

    var bar = document.createElement("div");
    bar.className = "asbuilt-navbar";
    var html = "";
    if (home) html += '<a class="ab-nav-home" href="' + escAttr(home) + '">◀ ' + escAttr(app || "Back to app") + "</a>";
    if (nav && nav.length) {
      html += '<nav class="ab-nav-links">' + nav.map(function (x) {
        return '<a href="' + escAttr(x.h) + '">' + escAttr(x.l) + "</a>";
      }).join("") + "</nav>";
    }
    bar.innerHTML = html;
    document.body.insertBefore(bar, document.body.firstChild);

    // Make room: push the sticky IDEA header (and thus the whole board) below us.
    var h = bar.offsetHeight || 40;
    document.body.style.paddingTop = h + "px";
    var hdr = document.querySelector("header");
    if (hdr && getComputedStyle(hdr).position === "sticky") hdr.style.top = h + "px";
    scheduleRedraw();  // board shifted → realign overlay lines
  }

  function renderControl() {
    if (document.querySelector(".asbuilt-ctl")) return;
    var m = (state && state.meta) || {};
    var box = document.createElement("div");
    box.className = "asbuilt-ctl";
    // (Back-to-app now lives in the top nav bar, renderTopBar().)
    box.innerHTML =
      '<div class="ab-title">AsBuilt overlay</div>' +
      '<div class="ab-row"><span class="ab-swatch core"></span>Core flow (animated)</div>' +
      '<div class="ab-row"><span class="ab-swatch feat"></span>Feature dependency</div>' +
      '<div class="ab-meta">' + (m.active_count || 0) + " active · " +
      (m.core_edge_count || 0) + " core · " + (m.feature_edge_count || 0) + " feature</div>" +
      '<div class="ab-meta">' + (m.profile || "") + (m.idea_release ? " · " + m.idea_release : "") + "</div>" +
      '<div class="ab-slider"><label>Line opacity <span id="ab-op-val">100%</span></label>' +
      '<input type="range" id="ab-op" min="0" max="100" value="100"></div>' +
      '<div class="ab-btns"><button type="button" id="ab-toggle">Hide lines</button>' +
      '<button type="button" id="ab-flow">Flow: on</button></div>';
    document.body.appendChild(box);

    var op = box.querySelector("#ab-op");
    var opVal = box.querySelector("#ab-op-val");
    op.addEventListener("input", function () {
      var v = op.value / 100;
      document.documentElement.style.setProperty("--ab-line-opacity", v);
      opVal.textContent = op.value + "%";
    });
    box.querySelector("#ab-toggle").addEventListener("click", function () {
      var hidden = svg && svg.style.display === "none";
      if (svg) svg.style.display = hidden ? "" : "none";
      this.textContent = hidden ? "Hide lines" : "Show lines";
    });
    box.querySelector("#ab-flow").addEventListener("click", function () {
      var off = document.body.classList.toggle("asbuilt-noflow");
      this.textContent = off ? "Flow: off" : "Flow: on";
      if (off) stopFlow(); else startFlow();
    });
  }

  // Redraws are THROTTLED (≥250ms apart) and suppress observer reentrancy, so a
  // chatty trigger (IDEA re-fitting, ResizeObserver, etc.) can't spin drawEdges
  // every frame — which would recreate the flow pulses continuously and make the
  // animation look frozen. The rAF flow loop runs independently between redraws.
  var lastRedraw = 0, redrawTimer = null, suppressObs = false;
  function doRedraw() {
    redrawTimer = null;
    lastRedraw = Date.now();
    suppressObs = true;
    applyStates();
    drawEdges();
    setTimeout(function () { suppressObs = false; }, 0);  // release after mutations settle
  }
  function scheduleRedraw() {
    if (suppressObs || redrawTimer) return;
    redrawTimer = setTimeout(doRedraw, Math.max(0, 250 - (Date.now() - lastRedraw)));
  }

  // ---- load + observe ----------------------------------------------------
  function fetchState(urls, i) {
    i = i || 0;
    if (i >= urls.length) { console.warn("[asbuilt] no state file found"); return; }
    fetch(urls[i], { cache: "no-store" })
      .then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(function (json) { state = json; boot(); })
      .catch(function () { fetchState(urls, i + 1); });
  }

  // Merge our discovered evidence into IDEA's OWN detail drawer (#d-body), which
  // the base rewrites on each openDetail(). We append a section rather than
  // replace, so the base's "how it's used" + related content is preserved.
  function wireEvidence() {
    // capture-phase: record which box was clicked before the base opens the drawer
    document.addEventListener("click", function (e) {
      var b = e.target.closest && e.target.closest("button[data-id]");
      if (b) lastDetailId = b.getAttribute("data-id");
    }, true);

    var body = document.getElementById("d-body");
    if (!body) return;

    function esc(s) {
      return String(s).replace(/[&<>"]/g, function (c) {
        return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
      });
    }
    // Recursive collapsible tree using native <details>/<summary> (no extra JS).
    function renderTree(nodes) {
      var h = "<ul class='asbuilt-tree'>";
      nodes.forEach(function (n) {
        var meta = n.meta ? " <span class='ab-t-meta'>" + esc(n.meta) + "</span>" : "";
        if (n.children && n.children.length) {
          h += "<li><details><summary>" + esc(n.label) + meta +
               " <span class='ab-t-count'>(" + n.children.length + ")</span></summary>" +
               renderTree(n.children) + "</details></li>";
        } else {
          h += "<li class='ab-t-leaf'>" + esc(n.label) + meta + "</li>";
        }
      });
      return h + "</ul>";
    }

    function inject() {
      if (!state || !lastDetailId) return;
      if (body.querySelector(".asbuilt-evidence")) return;      // already injected this open
      var ev = (state.evidence || {})[lastDetailId];
      var tree = (state.details || {})[lastDetailId];
      if ((!ev || !ev.length) && !(tree && tree.length)) return; // not a lit box
      var sec = document.createElement("div");
      sec.className = "asbuilt-evidence";
      var html = "<h4>● Discovered in this workspace</h4>";
      if (ev && ev.length) {
        html += "<ul>" + ev.map(function (x) { return "<li>" + esc(x) + "</li>"; }).join("") + "</ul>";
      }
      if (tree && tree.length) {
        html += "<details class='asbuilt-drill'><summary>Explore assets ▸</summary>" +
                renderTree(tree) + "</details>";
      }
      sec.innerHTML = html;
      body.appendChild(sec);
    }
    new MutationObserver(inject).observe(body, { childList: true });
  }

  // Switch the IDEA base to the workspace's cloud so the provider infra band
  // (Azure/AWS/GCP) shows the right services. Clicks the toolbar provider button
  // only if it isn't already active (the active one carries class "on").
  function setCloud() {
    var cloud = state && state.meta && state.meta.cloud;
    if (!cloud) return;
    var label = { aws: "AWS", azure: "Azure", gcp: "GCP" }[String(cloud).toLowerCase()];
    if (!label) return;
    var btn = [].slice.call(document.querySelectorAll("button"))
      .find(function (b) { return b.textContent.trim() === label; });
    if (btn && !btn.classList.contains("on")) btn.click();  // re-render observer redraws overlay
  }

  // --- Flow animation, driven by requestAnimationFrame (NOT CSS @keyframes) ---
  // CSS animations can be silently paused by OS "Reduce Motion"; a rAF loop that
  // sets stroke-dashoffset directly runs in any foreground tab and we control it.
  var flowRAF = null, flowOffset = 0;
  function flowTick() {
    _diag.ticks++;
    flowOffset -= 0.9;                        // ~54px/s at 60fps
    if (flowOffset <= -280) flowOffset = 0;   // multiple of the 28px dash period
    if (svg) {
      var ps = svg.querySelectorAll(".asbuilt-core-pulse");
      for (var i = 0; i < ps.length; i++) ps[i].setAttribute("stroke-dashoffset", flowOffset);
    }
    flowRAF = requestAnimationFrame(flowTick);
  }
  function _setPulseDisplay(v) {
    if (!svg) return;
    var ps = svg.querySelectorAll(".asbuilt-core-pulse");
    for (var i = 0; i < ps.length; i++) ps[i].style.display = v;   // inline overrides any cached CSS
  }
  function startFlow() { _diag.flowStart++; _setPulseDisplay("inline"); if (!flowRAF) flowRAF = requestAnimationFrame(flowTick); }
  function stopFlow() { _setPulseDisplay("none"); if (flowRAF) { cancelAnimationFrame(flowRAF); flowRAF = null; } }

  function boot() {
    setCloud();
    renderTopBar();
    applyStates();
    drawEdges();
    renderControl();
    observe();
    wireEvidence();
    if (!document.body.classList.contains("asbuilt-noflow")) startFlow();
  }

  var observersWired = false;
  function observe() {
    if (observersWired) return;
    observersWired = true;
    window.addEventListener("resize", scheduleRedraw);
    var board = boardEl();
    if (window.ResizeObserver && board) {
      new ResizeObserver(scheduleRedraw).observe(board);
    }
    var fit = document.getElementById("fit-in");
    if (fit) {
      new MutationObserver(scheduleRedraw).observe(fit, { attributes: true, attributeFilter: ["style"] });
    }
    // Theme / shape / industry toggles live as body classes; box re-renders as
    // #board subtree childList changes. Re-apply states + redraw on either — but
    // IGNORE mutations inside our own overlay SVG (else we self-trigger a loop).
    new MutationObserver(scheduleRedraw).observe(document.body, { attributes: true, attributeFilter: ["class"] });
    if (board) {
      new MutationObserver(function (muts) {
        for (var i = 0; i < muts.length; i++) {
          var t = muts[i].target;
          if (svg && (t === svg || (svg.contains && svg.contains(t)))) continue; // our own edits
          return scheduleRedraw();
        }
      }).observe(board, { childList: true, subtree: true });
    }
  }

  function waitForBoard(tries) {
    tries = tries || 0;
    var board = boardEl();
    if (board && board.querySelector("button[data-id]")) {
      fetchState(STATE_URLS);
    } else if (tries < 100) {
      setTimeout(function () { waitForBoard(tries + 1); }, 100);
    }
  }

  window.AsBuilt = {
    refresh: function () { fetchState(STATE_URLS); },
    redraw: scheduleRedraw,
    diag: function () {
      _diag.pulses = svg ? svg.querySelectorAll(".asbuilt-core-pulse").length : 0;
      _diag.flowRAF = !!flowRAF;
      return _diag;
    },
    kick: function () { stopFlow(); startFlow(); }   // manual restart of the flow loop
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () { waitForBoard(); });
  } else {
    waitForBoard();
  }
})();

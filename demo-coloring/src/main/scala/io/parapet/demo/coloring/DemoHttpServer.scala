package io.parapet.demo.coloring

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

final class DemoHttpServer(simulation: GraphColoringSimulation, host: String = "127.0.0.1", port: Int = 8088):
  private val server = HttpServer.create(InetSocketAddress(host, port), 0)
  server.setExecutor(Executors.newCachedThreadPool())

  server.createContext("/", respond(indexHtml(), "text/html; charset=utf-8"))
  server.createContext("/app.js", respond(appJs(), "application/javascript; charset=utf-8"))
  server.createContext("/api/state", exchange => json(exchange, Json.render(simulation.snapshot())))
  server.createContext("/api/step", exchange => json(exchange, Json.render(simulation.stepRound())))
  server.createContext("/api/start", exchange => json(exchange, Json.render(simulation.setRunning(true))))
  server.createContext("/api/pause", exchange => json(exchange, Json.render(simulation.setRunning(false))))
  server.createContext("/api/reset", exchange => json(exchange, Json.render(simulation.reset())))
  server.createContext("/api/configure", exchange => json(exchange, Json.render(configure(exchange))))

  def start(): Unit =
    server.start()

  def stop(delaySeconds: Int = 0): Unit =
    server.stop(delaySeconds)

  def url: String =
    s"http://$host:$port/"

  private def configure(exchange: HttpExchange): DemoState =
    val query = parseQuery(exchange.getRequestURI.getRawQuery)
    val nodes = query.get("nodes").flatMap(_.toIntOption).getOrElse(simulation.snapshot().nodeCount)
    val colors = query.get("colors").flatMap(_.toIntOption).getOrElse(simulation.snapshot().paletteSize)
    simulation.configure(nodes, colors)

  private def parseQuery(raw: String | Null): Map[String, String] =
    Option(raw)
      .filter(_.nonEmpty)
      .map { value =>
        value.split("&").toVector.flatMap { pair =>
          pair.split("=", 2).toList match
            case key :: provided :: Nil =>
              Some(
                URLDecoder.decode(key, StandardCharsets.UTF_8) ->
                  URLDecoder.decode(provided, StandardCharsets.UTF_8)
              )
            case key :: Nil =>
              Some(URLDecoder.decode(key, StandardCharsets.UTF_8) -> "")
            case _ =>
              None
        }.toMap
      }
      .getOrElse(Map.empty)

  private def respond(body: String, contentType: String): HttpHandler =
    exchange => write(exchange, 200, body, contentType)

  private def json(exchange: HttpExchange, body: String): Unit =
    write(exchange, 200, body, "application/json; charset=utf-8")

  private def write(exchange: HttpExchange, code: Int, body: String, contentType: String): Unit =
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", contentType)
    exchange.getResponseHeaders.add("Cache-Control", "no-store")
    exchange.sendResponseHeaders(code, bytes.length)
    val os = exchange.getResponseBody
    try os.write(bytes)
    finally os.close()

  private def indexHtml(): String =
    """<!doctype html>
      |<html lang="en">
      |  <head>
      |    <meta charset="utf-8" />
      |    <meta name="viewport" content="width=device-width, initial-scale=1" />
      |    <title>Parapet Distributed Graph Lab</title>
      |    <style>
      |      :root {
      |        --bg: #f6f1e8;
      |        --panel: rgba(255,255,255,0.78);
      |        --ink: #20312f;
      |        --muted: #5f736d;
      |        --accent: #cf5c36;
      |        --accent-2: #14746f;
      |        --line: rgba(32,49,47,0.18);
      |      }
      |      * { box-sizing: border-box; }
      |      body {
      |        margin: 0;
      |        font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      |        color: var(--ink);
      |        background:
      |          radial-gradient(circle at top left, rgba(207,92,54,0.14), transparent 34%),
      |          radial-gradient(circle at bottom right, rgba(20,116,111,0.18), transparent 36%),
      |          var(--bg);
      |      }
      |      .layout {
      |        display: grid;
      |        grid-template-columns: minmax(0, 1.7fr) minmax(320px, 1fr);
      |        gap: 18px;
      |        min-height: 100vh;
      |        padding: 18px;
      |      }
      |      .panel {
      |        background: var(--panel);
      |        backdrop-filter: blur(8px);
      |        border: 1px solid rgba(255,255,255,0.65);
      |        border-radius: 24px;
      |        box-shadow: 0 18px 40px rgba(40, 40, 32, 0.08);
      |        overflow: hidden;
      |      }
      |      .canvas-shell { padding: 18px; }
      |      .title {
      |        display: flex;
      |        align-items: center;
      |        justify-content: space-between;
      |        gap: 18px;
      |        margin-bottom: 14px;
      |      }
      |      h1, h2, h3, p { margin: 0; }
      |      h1 { font-size: 2rem; line-height: 1.05; }
      |      .sub { color: var(--muted); margin-top: 6px; }
      |      .controls {
      |        display: flex;
      |        flex-wrap: wrap;
      |        gap: 10px;
      |        margin-top: 14px;
      |      }
      |      .controls-row {
      |        display: flex;
      |        flex-wrap: wrap;
      |        align-items: end;
      |        gap: 12px;
      |        margin-top: 14px;
      |      }
      |      .field {
      |        display: grid;
      |        gap: 6px;
      |      }
      |      .field label {
      |        color: var(--muted);
      |        font-size: 0.9rem;
      |      }
      |      input[type="number"] {
      |        min-width: 100px;
      |        border-radius: 14px;
      |        border: 1px solid rgba(32,49,47,0.15);
      |        padding: 10px 12px;
      |        font: inherit;
      |        background: rgba(255,255,255,0.9);
      |      }
      |      button {
      |        border: 0;
      |        border-radius: 999px;
      |        padding: 11px 16px;
      |        font: inherit;
      |        cursor: pointer;
      |        color: white;
      |        background: var(--accent-2);
      |        box-shadow: 0 10px 18px rgba(20,116,111,0.18);
      |      }
      |      button.secondary { background: #876445; box-shadow: 0 10px 18px rgba(135,100,69,0.16); }
      |      button.warning { background: var(--accent); box-shadow: 0 10px 18px rgba(207,92,54,0.16); }
      |      .stats {
      |        display: grid;
      |        grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
      |        gap: 10px;
      |        margin: 16px 0;
      |      }
      |      .stat {
      |        padding: 12px 14px;
      |        border-radius: 18px;
      |        background: rgba(255,255,255,0.66);
      |        border: 1px solid rgba(32,49,47,0.08);
      |      }
      |      .stat .label { display: block; color: var(--muted); font-size: 0.9rem; }
      |      .stat .value { display: block; margin-top: 4px; font-size: 1.3rem; font-weight: 600; }
      |      .graph-shell {
      |        position: relative;
      |      }
      |      .zoom-bar {
      |        position: absolute;
      |        top: 14px;
      |        right: 14px;
      |        display: flex;
      |        gap: 8px;
      |        z-index: 1;
      |      }
      |      .zoom-bar button {
      |        padding: 9px 13px;
      |        border-radius: 12px;
      |      }
      |      svg {
      |        width: 100%;
      |        height: auto;
      |        display: block;
      |        border-radius: 18px;
      |        background: rgba(255,255,255,0.55);
      |        touch-action: none;
      |        cursor: grab;
      |      }
      |      svg.dragging { cursor: grabbing; }
      |      .sidebar {
      |        display: grid;
      |        grid-template-rows: auto auto 1fr;
      |        gap: 18px;
      |        padding: 18px;
      |      }
      |      .list {
      |        display: grid;
      |        gap: 10px;
      |        max-height: 280px;
      |        overflow: auto;
      |      }
      |      .card {
      |        padding: 14px;
      |        border-radius: 16px;
      |        background: rgba(255,255,255,0.66);
      |        border: 1px solid rgba(32,49,47,0.08);
      |      }
      |      .event-log {
      |        max-height: 360px;
      |        overflow: auto;
      |        display: grid;
      |        gap: 8px;
      |      }
      |      .event {
      |        padding: 12px 14px;
      |        border-radius: 14px;
      |        background: rgba(255,255,255,0.72);
      |        border-left: 4px solid var(--accent-2);
      |      }
      |      .event[data-kind="conflict"] { border-left-color: var(--accent); }
      |      .event[data-kind="lock"] { border-left-color: #7c9d3a; }
      |      .muted { color: var(--muted); }
      |      @media (max-width: 980px) {
      |        .layout { grid-template-columns: 1fr; }
      |      }
      |    </style>
      |  </head>
      |  <body>
      |    <div class="layout">
      |      <section class="panel canvas-shell">
      |        <div class="title">
      |          <div>
      |            <h1>Distributed Graph Lab</h1>
      |            <p class="sub">Randomized graph coloring with live controls. Use the wheel to zoom, drag to pan, and regenerate larger graphs like 100 processes.</p>
      |          </div>
      |          <div class="muted" id="graph-id"></div>
      |        </div>
      |        <div class="controls">
      |          <button id="start-btn">Auto Run</button>
      |          <button class="secondary" id="pause-btn">Pause</button>
      |          <button class="warning" id="step-btn">Step Round</button>
      |          <button class="secondary" id="reset-btn">Reset</button>
      |        </div>
      |        <div class="controls-row">
      |          <div class="field">
      |            <label for="nodes-input">Processes</label>
      |            <input id="nodes-input" type="number" min="4" max="140" value="12" />
      |          </div>
      |          <div class="field">
      |            <label for="colors-input">Colors</label>
      |            <input id="colors-input" type="number" min="2" max="12" value="4" />
      |          </div>
      |          <button id="apply-btn">Apply Graph</button>
      |          <div class="muted">Bigger graphs stay colorable by construction so the demo remains usable.</div>
      |        </div>
      |        <div class="stats">
      |          <div class="stat"><span class="label">Round</span><span class="value" id="round-value">0</span></div>
      |          <div class="stat"><span class="label">Colored</span><span class="value" id="colored-value">0</span></div>
      |          <div class="stat"><span class="label">Conflicts</span><span class="value" id="conflicts-value">0</span></div>
      |          <div class="stat"><span class="label">Processes</span><span class="value" id="nodes-value">0</span></div>
      |          <div class="stat"><span class="label">Palette</span><span class="value" id="palette-value">0</span></div>
      |        </div>
      |        <div class="graph-shell">
      |          <div class="zoom-bar">
      |            <button class="secondary" id="zoom-out-btn">-</button>
      |            <button class="secondary" id="zoom-reset-btn">Reset View</button>
      |            <button class="secondary" id="zoom-in-btn">+</button>
      |          </div>
      |          <svg id="graph" viewBox="0 0 760 600" aria-label="graph visualization"></svg>
      |        </div>
      |      </section>
      |      <aside class="panel sidebar">
      |        <section>
      |          <h2>Cluster</h2>
      |          <p class="sub">Placeholder control plane panel for now. This will later reflect real remote Raft workers.</p>
      |          <div class="list" id="cluster-list"></div>
      |        </section>
      |        <section>
      |          <h2>Status</h2>
      |          <div class="card">
      |            <div id="status-copy" class="muted">Waiting for state...</div>
      |          </div>
      |        </section>
      |        <section>
      |          <h2>Event Stream</h2>
      |          <div class="event-log" id="event-log"></div>
      |        </section>
      |      </aside>
      |    </div>
      |    <script src="/app.js" defer></script>
      |  </body>
      |</html>
      |""".stripMargin

  private def appJs(): String =
    """const palette = ["#6d597a", "#e76f51", "#2a9d8f", "#e9c46a", "#457b9d", "#90be6d", "#b56576", "#355070", "#588157", "#f28482", "#8ecae6", "#ffb703"];
      |let autoTimer = null;
      |let latestState = null;
      |const viewState = { scale: 1, offsetX: 0, offsetY: 0, dragging: false, dragStartX: 0, dragStartY: 0 };
      |
      |async function call(endpoint) {
      |  const response = await fetch(endpoint, { method: "POST" });
      |  return response.json();
      |}
      |
      |async function fetchState() {
      |  const response = await fetch("/api/state");
      |  return response.json();
      |}
      |
      |function edgeKey(a, b) {
      |  return a < b ? `${a}:${b}` : `${b}:${a}`;
      |}
      |
      |function nodeRadius(state) {
      |  return Math.max(8, 24 - Math.floor(state.nodeCount / 8));
      |}
      |
      |function resetView() {
      |  viewState.scale = 1;
      |  viewState.offsetX = 0;
      |  viewState.offsetY = 0;
      |  applyViewport();
      |}
      |
      |function applyViewport() {
      |  const viewport = document.getElementById("graph-viewport");
      |  const svg = document.getElementById("graph");
      |  if (!viewport || !svg) return;
      |  viewport.setAttribute("transform", `translate(${viewState.offsetX} ${viewState.offsetY}) scale(${viewState.scale})`);
      |  svg.classList.toggle("dragging", viewState.dragging);
      |}
      |
      |function renderGraph(state) {
      |  const svg = document.getElementById("graph");
      |  const radius = nodeRadius(state);
      |  const nodesById = Object.fromEntries(state.nodes.map(node => [node.id, node]));
      |  const edges = new Set();
      |  let lines = "";
      |  let circles = "";
      |  let labels = "";
      |
      |  svg.setAttribute("viewBox", `0 0 ${state.canvasWidth} ${state.canvasHeight}`);
      |
      |  state.nodes.forEach(node => {
      |    node.neighbors.forEach(neighborId => {
      |      const neighbor = nodesById[neighborId];
      |      if (!neighbor) return;
      |      const key = edgeKey(node.id, neighborId);
      |      if (edges.has(key)) return;
      |      edges.add(key);
      |      lines += `<line x1="${node.x}" y1="${node.y}" x2="${neighbor.x}" y2="${neighbor.y}" stroke="rgba(32,49,47,0.18)" stroke-width="${Math.max(1, radius / 8)}" />`;
      |    });
      |  });
      |
      |  state.nodes.forEach(node => {
      |    const fill = node.color == null ? "rgba(255,255,255,0.86)" : palette[node.color % palette.length];
      |    const stroke = node.conflict ? "#cf5c36" : "#20312f";
      |    const strokeWidth = node.conflict ? Math.max(3, radius / 3) : Math.max(1.5, radius / 10);
      |    circles += `<circle cx="${node.x}" cy="${node.y}" r="${radius}" fill="${fill}" stroke="${stroke}" stroke-width="${strokeWidth}" />`;
      |    if (state.nodeCount <= 50) {
      |      labels += `<text x="${node.x}" y="${node.y + 4}" text-anchor="middle" font-size="${Math.max(8, radius * 0.58)}" fill="#20312f">${node.id}</text>`;
      |    }
      |    if (node.proposedColor != null && state.nodeCount <= 80) {
      |      labels += `<text x="${node.x}" y="${node.y - radius - 8}" text-anchor="middle" font-size="${Math.max(8, radius * 0.52)}" fill="#5f736d">p:${node.proposedColor}</text>`;
      |    }
      |  });
      |
      |  svg.innerHTML = `<g id="graph-viewport">${lines}${circles}${labels}</g>`;
      |  applyViewport();
      |}
      |
      |function renderCluster(state) {
      |  const root = document.getElementById("cluster-list");
      |  const limit = state.cluster.length > 24 ? 24 : state.cluster.length;
      |  const visible = state.cluster.slice(0, limit);
      |  const overflow = state.cluster.length - visible.length;
      |  root.innerHTML = visible.map(node => {
      |    return `
      |      <div class="card">
      |        <strong>${node.id}</strong>
      |        <div class="muted">${node.address}</div>
      |        <div>Role: ${node.role}</div>
      |        <div>Term: ${node.term}</div>
      |        <div>Online: ${node.online}</div>
      |      </div>
      |    `;
      |  }).join("") + (overflow > 0 ? `<div class="card muted">... and ${overflow} more workers</div>` : "");
      |}
      |
      |function renderEvents(state) {
      |  const root = document.getElementById("event-log");
      |  root.innerHTML = state.events.slice().reverse().map(event => {
      |    const target = event.nodeId ? `<div class="muted">${event.nodeId}</div>` : "";
      |    return `<div class="event" data-kind="${event.kind}">
      |      <strong>${event.kind}</strong>
      |      ${target}
      |      <div>${event.detail}</div>
      |    </div>`;
      |  }).join("");
      |}
      |
      |function renderState(state) {
      |  latestState = state;
      |  document.getElementById("graph-id").textContent = state.graphId;
      |  document.getElementById("round-value").textContent = state.round;
      |  document.getElementById("colored-value").textContent = state.nodes.filter(node => node.color != null).length;
      |  document.getElementById("conflicts-value").textContent = state.nodes.filter(node => node.conflict).length;
      |  document.getElementById("palette-value").textContent = state.paletteSize;
      |  document.getElementById("nodes-value").textContent = state.nodeCount;
      |  document.getElementById("nodes-input").value = state.nodeCount;
      |  document.getElementById("colors-input").value = state.paletteSize;
      |  document.getElementById("status-copy").textContent = state.completed
      |    ? `Completed in ${state.round} rounds with ${state.nodeCount} processes`
      |    : state.running
      |      ? "Auto-run active. Mouse wheel zooms, drag pans."
      |      : "Paused. Adjust process/color counts, then step or auto-run.";
      |  renderGraph(state);
      |  renderCluster(state);
      |  renderEvents(state);
      |}
      |
      |async function refresh() {
      |  renderState(await fetchState());
      |}
      |
      |function startAutoRun() {
      |  clearInterval(autoTimer);
      |  autoTimer = setInterval(async () => {
      |    const state = await call("/api/step");
      |    renderState(state);
      |    if (state.completed) {
      |      stopAutoRun();
      |    }
      |  }, 800);
      |}
      |
      |function stopAutoRun() {
      |  if (autoTimer) {
      |    clearInterval(autoTimer);
      |    autoTimer = null;
      |  }
      |}
      |
      |function zoomBy(factor, centerX, centerY) {
      |  const svg = document.getElementById("graph");
      |  const rect = svg.getBoundingClientRect();
      |  const viewBox = svg.viewBox.baseVal;
      |  const pointerX = centerX ?? rect.width / 2;
      |  const pointerY = centerY ?? rect.height / 2;
      |  const svgX = pointerX * (viewBox.width / rect.width);
      |  const svgY = pointerY * (viewBox.height / rect.height);
      |  const newScale = Math.max(0.35, Math.min(4.5, viewState.scale * factor));
      |  const worldX = (svgX - viewState.offsetX) / viewState.scale;
      |  const worldY = (svgY - viewState.offsetY) / viewState.scale;
      |  viewState.scale = newScale;
      |  viewState.offsetX = svgX - worldX * newScale;
      |  viewState.offsetY = svgY - worldY * newScale;
      |  applyViewport();
      |}
      |
      |function attachGraphInteractions() {
      |  const svg = document.getElementById("graph");
      |  svg.addEventListener("wheel", event => {
      |    event.preventDefault();
      |    zoomBy(event.deltaY < 0 ? 1.14 : 0.88, event.offsetX, event.offsetY);
      |  }, { passive: false });
      |
      |  svg.addEventListener("pointerdown", event => {
      |    viewState.dragging = true;
      |    viewState.dragStartX = event.clientX;
      |    viewState.dragStartY = event.clientY;
      |    svg.setPointerCapture(event.pointerId);
      |    applyViewport();
      |  });
      |
      |  svg.addEventListener("pointermove", event => {
      |    if (!viewState.dragging) return;
      |    const rect = svg.getBoundingClientRect();
      |    const viewBox = svg.viewBox.baseVal;
      |    const dx = (event.clientX - viewState.dragStartX) * (viewBox.width / rect.width);
      |    const dy = (event.clientY - viewState.dragStartY) * (viewBox.height / rect.height);
      |    viewState.offsetX += dx;
      |    viewState.offsetY += dy;
      |    viewState.dragStartX = event.clientX;
      |    viewState.dragStartY = event.clientY;
      |    applyViewport();
      |  });
      |
      |  svg.addEventListener("pointerup", event => {
      |    viewState.dragging = false;
      |    svg.releasePointerCapture(event.pointerId);
      |    applyViewport();
      |  });
      |
      |  svg.addEventListener("pointerleave", () => {
      |    if (!viewState.dragging) return;
      |    viewState.dragging = false;
      |    applyViewport();
      |  });
      |}
      |
      |document.getElementById("start-btn").addEventListener("click", async () => {
      |  renderState(await call("/api/start"));
      |  startAutoRun();
      |});
      |
      |document.getElementById("pause-btn").addEventListener("click", async () => {
      |  stopAutoRun();
      |  renderState(await call("/api/pause"));
      |});
      |
      |document.getElementById("step-btn").addEventListener("click", async () => {
      |  stopAutoRun();
      |  renderState(await call("/api/step"));
      |});
      |
      |document.getElementById("reset-btn").addEventListener("click", async () => {
      |  stopAutoRun();
      |  resetView();
      |  renderState(await call("/api/reset"));
      |});
      |
      |document.getElementById("apply-btn").addEventListener("click", async () => {
      |  stopAutoRun();
      |  resetView();
      |  const nodes = document.getElementById("nodes-input").value;
      |  const colors = document.getElementById("colors-input").value;
      |  renderState(await call(`/api/configure?nodes=${encodeURIComponent(nodes)}&colors=${encodeURIComponent(colors)}`));
      |});
      |
      |document.getElementById("zoom-in-btn").addEventListener("click", () => zoomBy(1.16));
      |document.getElementById("zoom-out-btn").addEventListener("click", () => zoomBy(0.86));
      |document.getElementById("zoom-reset-btn").addEventListener("click", () => resetView());
      |
      |attachGraphInteractions();
      |refresh();
      |""".stripMargin

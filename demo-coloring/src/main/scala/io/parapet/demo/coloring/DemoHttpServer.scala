package io.parapet.demo.coloring

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

/** Minimal HTTP front-end for [[GraphColoringSimulation]].
  *
  * Built directly on the JDK's `com.sun.net.httpserver` to keep the demo dependency-free. Serves three kinds of
  * resources:
  *
  *   - `GET /` - the single-page UI HTML.
  *   - `GET /app.js` - the front-end JavaScript driving the 3D force graph.
  *   - `GET /api/...` - JSON endpoints wrapping individual simulation actions (`state`, `step`, `start`, `pause`,
  *     `reset`, `configure`, `burst`, `battle/start`, `battle/stop`); each returns the post-call [[DemoState]].
  *
  * @param simulation
  *   engine the server fronts.
  * @param host
  *   interface to bind; defaults to loopback.
  * @param port
  *   TCP port to listen on; defaults to 8088.
  */
final class DemoHttpServer(simulation: GraphColoringSimulation, host: String = "127.0.0.1", port: Int = 8088):
  private val server = HttpServer.create(InetSocketAddress(host, port), 0)
  server.setExecutor(Executors.newCachedThreadPool())

  server.createContext("/", respond(indexHtml(), "text/html; charset=utf-8"))
  server.createContext("/app.js", respond(appJs(), "application/javascript; charset=utf-8"))
  server.createContext("/api/state", exchange => json(exchange, Json.render(simulation.snapshot())))
  server.createContext("/api/step", exchange => json(exchange, Json.render(simulation.step())))
  server.createContext("/api/start", exchange => json(exchange, Json.render(simulation.setRunning(true))))
  server.createContext("/api/pause", exchange => json(exchange, Json.render(simulation.setRunning(false))))
  server.createContext("/api/reset", exchange => json(exchange, Json.render(simulation.reset())))
  server.createContext("/api/configure", exchange => json(exchange, Json.render(configure(exchange))))
  server.createContext("/api/burst", exchange => json(exchange, Json.render(burst(exchange))))
  server.createContext("/api/battle/start", exchange => json(exchange, Json.render(simulation.startBattle())))
  server.createContext("/api/battle/stop", exchange => json(exchange, Json.render(simulation.stopBattle())))

  /** Start accepting connections on the configured host/port. */
  def start(): Unit =
    server.start()

  /** Stop the server, allowing in-flight requests up to `delaySeconds` to complete. */
  def stop(delaySeconds: Int = 0): Unit =
    server.stop(delaySeconds)

  /** Base URL the front-end and API are served from. */
  def url: String =
    s"http://$host:$port/"

  private def configure(exchange: HttpExchange): DemoState =
    val query  = parseQuery(exchange.getRequestURI.getRawQuery)
    val nodes  = query.get("nodes").flatMap(_.toIntOption).getOrElse(simulation.snapshot().nodeCount)
    val colors = query.get("colors").flatMap(_.toIntOption).getOrElse(simulation.snapshot().paletteSize)
    simulation.configure(nodes, colors)

  private def burst(exchange: HttpExchange): DemoState =
    val query   = parseQuery(exchange.getRequestURI.getRawQuery)
    val size    = query.get("size").flatMap(_.toIntOption).getOrElse(8)
    val bridges = query.get("bridges").flatMap(_.toIntOption).getOrElse(1)
    simulation.burst(size, bridges)

  private def parseQuery(raw: String | Null): Map[String, String] =
    Option(raw)
      .filter(_.nonEmpty)
      .map { value =>
        value
          .split("&")
          .toVector
          .flatMap { pair =>
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
          }
          .toMap
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
      |    <title>Parapet · Distributed Graph Lab</title>
      |    <style>
      |      :root {
      |        --bg: #05070d;
      |        --bg-2: #0b111b;
      |        --panel: rgba(12, 18, 28, 0.72);
      |        --ink: #e8eef5;
      |        --muted: #8797ab;
      |        --accent: #ff8b5a;
      |        --accent-2: #59e0c4;
      |        --accent-3: #6ab8ff;
      |        --line: rgba(255,255,255,0.08);
      |      }
      |      * { box-sizing: border-box; }
      |      html, body { height: 100%; }
      |      body {
      |        margin: 0;
      |        font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      |        color: var(--ink);
      |        background:
      |          radial-gradient(1200px 600px at 10% -10%, rgba(106,184,255,0.14), transparent 60%),
      |          radial-gradient(900px 500px at 110% 110%, rgba(89,224,196,0.12), transparent 55%),
      |          linear-gradient(180deg, var(--bg), var(--bg-2));
      |        overflow: hidden;
      |      }
      |      .layout {
      |        display: grid;
      |        grid-template-columns: minmax(0, 1fr) 360px;
      |        gap: 14px;
      |        height: 100vh;
      |        padding: 14px;
      |      }
      |      .panel {
      |        background: var(--panel);
      |        border: 1px solid var(--line);
      |        border-radius: 18px;
      |        box-shadow: 0 20px 50px rgba(0,0,0,0.4);
      |        backdrop-filter: blur(10px);
      |        -webkit-backdrop-filter: blur(10px);
      |        overflow: hidden;
      |      }
      |      .canvas-shell {
      |        display: grid;
      |        grid-template-rows: auto auto auto 1fr;
      |        padding: 16px;
      |        gap: 12px;
      |        min-height: 0;
      |      }
      |      .title { display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }
      |      h1 { font-size: 1.35rem; letter-spacing: 0.02em; margin: 0; font-weight: 600; }
      |      .sub { color: var(--muted); font-size: 0.82rem; margin-top: 4px; }
      |      .chip {
      |        font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, monospace;
      |        font-size: 0.72rem;
      |        color: var(--muted);
      |        padding: 4px 10px;
      |        border-radius: 999px;
      |        border: 1px solid var(--line);
      |      }
      |      .controls { display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }
      |      .field { display: grid; gap: 4px; }
      |      .field label {
      |        color: var(--muted);
      |        font-size: 0.7rem;
      |        text-transform: uppercase;
      |        letter-spacing: 0.08em;
      |      }
      |      input[type="number"] {
      |        min-width: 86px;
      |        background: rgba(255,255,255,0.04);
      |        border: 1px solid var(--line);
      |        color: var(--ink);
      |        padding: 8px 10px;
      |        border-radius: 10px;
      |        font: inherit;
      |      }
      |      button {
      |        border: 1px solid var(--line);
      |        color: var(--ink);
      |        background: rgba(255,255,255,0.04);
      |        padding: 8px 14px;
      |        border-radius: 999px;
      |        cursor: pointer;
      |        font: inherit;
      |        transition: transform 80ms ease, background 160ms ease, border-color 160ms ease;
      |      }
      |      button:hover { background: rgba(255,255,255,0.08); transform: translateY(-1px); }
      |      button.primary { background: linear-gradient(135deg, #ff8b5a, #ff5a8b); border-color: transparent; color: #fff; }
      |      button.secondary { background: rgba(106,184,255,0.12); border-color: rgba(106,184,255,0.25); }
      |      button.accent { background: rgba(89,224,196,0.12); border-color: rgba(89,224,196,0.25); }
      |      button.toggle[aria-pressed="true"] {
      |        background: linear-gradient(135deg, #59e0c4, #6ab8ff);
      |        border-color: transparent;
      |        color: #05070d;
      |        font-weight: 600;
      |      }
      |      .stats {
      |        display: grid;
      |        grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
      |        gap: 8px;
      |      }
      |      .stat {
      |        padding: 10px 12px;
      |        border-radius: 12px;
      |        background: rgba(255,255,255,0.03);
      |        border: 1px solid var(--line);
      |      }
      |      .stat .label {
      |        color: var(--muted);
      |        font-size: 0.7rem;
      |        text-transform: uppercase;
      |        letter-spacing: 0.06em;
      |      }
      |      .stat .value {
      |        font-size: 1.15rem;
      |        font-weight: 600;
      |        margin-top: 2px;
      |        font-variant-numeric: tabular-nums;
      |      }
      |      .graph-shell {
      |        position: relative;
      |        border-radius: 12px;
      |        overflow: hidden;
      |        border: 1px solid var(--line);
      |        background: #03060c;
      |        min-height: 0;
      |      }
      |      .graph-shell #graph { position: absolute; inset: 0; }
      |      .legend {
      |        position: absolute;
      |        left: 12px;
      |        bottom: 12px;
      |        display: flex;
      |        gap: 10px;
      |        flex-wrap: wrap;
      |        padding: 8px 10px;
      |        background: rgba(5,7,13,0.65);
      |        border: 1px solid var(--line);
      |        border-radius: 12px;
      |        font-size: 0.72rem;
      |        color: var(--muted);
      |        z-index: 2;
      |      }
      |      .legend .dot {
      |        display: inline-block;
      |        width: 10px;
      |        height: 10px;
      |        border-radius: 50%;
      |        margin-right: 6px;
      |        vertical-align: middle;
      |      }
      |      .sidebar {
      |        display: grid;
      |        grid-template-rows: auto auto 1fr;
      |        gap: 12px;
      |        padding: 16px;
      |        overflow: hidden;
      |        min-height: 0;
      |      }
      |      .sidebar section { min-height: 0; display: flex; flex-direction: column; }
      |      .sidebar h2 {
      |        font-size: 0.8rem;
      |        text-transform: uppercase;
      |        letter-spacing: 0.1em;
      |        color: var(--muted);
      |        margin: 0 0 8px 0;
      |        font-weight: 600;
      |      }
      |      .status-card {
      |        padding: 12px;
      |        border-radius: 12px;
      |        background: rgba(255,255,255,0.03);
      |        border: 1px solid var(--line);
      |        color: var(--muted);
      |        font-size: 0.85rem;
      |      }
      |      .event-log {
      |        overflow: auto;
      |        display: grid;
      |        gap: 6px;
      |        padding-right: 4px;
      |        min-height: 0;
      |      }
      |      .event {
      |        padding: 8px 10px;
      |        border-radius: 10px;
      |        background: rgba(255,255,255,0.03);
      |        border-left: 3px solid var(--accent-3);
      |        font-size: 0.78rem;
      |      }
      |      .event[data-kind="conflict"] { border-left-color: var(--accent); }
      |      .event[data-kind="lock"] { border-left-color: var(--accent-2); }
      |      .event[data-kind="burst"] { border-left-color: #ffd15c; }
      |      .event[data-kind="battle"] { border-left-color: #ff4e50; }
      |      .event[data-kind="conquer"] { border-left-color: #ff4e50; }
      |      .event[data-kind="defend"] { border-left-color: #6ab8ff; }
      |      .event[data-kind="victory"] { border-left-color: #ffd15c; background: rgba(255,209,92,0.12); }
      |      .event[data-kind="stalemate"] { border-left-color: #8797ab; }
      |      .race-board {
      |        display: grid;
      |        gap: 6px;
      |      }
      |      .race-row {
      |        display: grid;
      |        grid-template-columns: 28px 1fr auto;
      |        align-items: center;
      |        gap: 8px;
      |        font-size: 0.8rem;
      |      }
      |      .race-row .swatch {
      |        width: 20px;
      |        height: 20px;
      |        border-radius: 5px;
      |        border: 1px solid rgba(255,255,255,0.15);
      |      }
      |      .race-row .bar {
      |        position: relative;
      |        height: 8px;
      |        border-radius: 4px;
      |        background: rgba(255,255,255,0.06);
      |        overflow: hidden;
      |      }
      |      .race-row .bar-fill {
      |        position: absolute;
      |        inset: 0 auto 0 0;
      |        border-radius: 4px;
      |        transition: width 260ms ease;
      |      }
      |      .race-row .count {
      |        font-variant-numeric: tabular-nums;
      |        color: var(--muted);
      |      }
      |      .race-row.winner .count { color: #ffd15c; font-weight: 600; }
      |      .mode-chip {
      |        font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, monospace;
      |        font-size: 0.7rem;
      |        padding: 3px 9px;
      |        border-radius: 999px;
      |        border: 1px solid var(--line);
      |        background: rgba(255,255,255,0.04);
      |        color: var(--muted);
      |        margin-left: 8px;
      |      }
      |      .mode-chip.battle { background: rgba(255,78,80,0.15); border-color: rgba(255,78,80,0.3); color: #ffadad; }
      |      body.mode-battle .canvas-shell { box-shadow: inset 0 0 0 1px rgba(255,78,80,0.35); }
      |      .event strong {
      |        text-transform: uppercase;
      |        font-size: 0.7rem;
      |        letter-spacing: 0.08em;
      |        color: var(--muted);
      |        margin-right: 6px;
      |      }
      |      .event .detail { margin-top: 2px; color: var(--ink); }
      |      .event .target { color: var(--muted); font-family: ui-monospace, SFMono-Regular, monospace; }
      |      @media (max-width: 1100px) {
      |        .layout { grid-template-columns: 1fr; }
      |        .sidebar { display: none; }
      |      }
      |    </style>
      |  </head>
      |  <body>
      |    <div class="layout">
      |      <section class="panel canvas-shell">
      |        <header class="title">
      |          <div>
      |            <h1>Parapet · Distributed Graph Lab<span class="mode-chip" id="mode-chip">coloring</span></h1>
      |            <div class="sub">Randomized graph coloring in 3D. Drag to orbit, scroll to zoom, right-click to pan. Spawn clusters, then start the battle.</div>
      |          </div>
      |          <div class="chip" id="graph-id">-</div>
      |        </header>
      |        <div class="controls">
      |          <button class="primary" id="start-btn">Auto Run</button>
      |          <button class="secondary" id="step-btn">Step</button>
      |          <button id="pause-btn">Pause</button>
      |          <button id="reset-btn">Reset</button>
      |          <span style="width: 6px;"></span>
      |          <div class="field">
      |            <label for="nodes-input">Processes</label>
      |            <input id="nodes-input" type="number" min="4" max="2000" step="1" value="200" />
      |          </div>
      |          <div class="field">
      |            <label for="colors-input">Colors</label>
      |            <input id="colors-input" type="number" min="2" max="12" step="1" value="5" />
      |          </div>
      |          <button class="accent" id="apply-btn">Apply</button>
      |          <span style="width: 6px;"></span>
      |          <div class="field">
      |            <label for="burst-input">Cluster size</label>
      |            <input id="burst-input" type="number" min="2" max="60" step="1" value="10" />
      |          </div>
      |          <div class="field">
      |            <label for="bridges-input">Bridges</label>
      |            <input id="bridges-input" type="number" min="1" max="6" step="1" value="1" />
      |          </div>
      |          <button class="accent" id="burst-btn">Spawn Cluster</button>
      |          <button class="toggle" id="auto-burst-btn" aria-pressed="false">Auto Spawn</button>
      |          <span style="width: 6px;"></span>
      |          <button class="toggle" id="battle-btn" aria-pressed="false">Start Battle</button>
      |        </div>
      |        <div class="stats">
      |          <div class="stat"><div class="label">Round</div><div class="value" id="round-value">0</div></div>
      |          <div class="stat"><div class="label">Colored</div><div class="value" id="colored-value">0</div></div>
      |          <div class="stat"><div class="label">Conflicts</div><div class="value" id="conflicts-value">0</div></div>
      |          <div class="stat"><div class="label">Processes</div><div class="value" id="nodes-value">0</div></div>
      |          <div class="stat"><div class="label">Clusters</div><div class="value" id="clusters-value">0</div></div>
      |          <div class="stat"><div class="label">Locked</div><div class="value" id="locked-value">0</div></div>
      |          <div class="stat"><div class="label">Palette</div><div class="value" id="palette-value">0</div></div>
      |        </div>
      |        <div class="graph-shell" id="graph-shell">
      |          <div id="graph"></div>
      |          <div class="legend" id="legend"></div>
      |        </div>
      |      </section>
      |      <aside class="panel sidebar">
      |        <section>
      |          <h2>Status</h2>
      |          <div class="status-card" id="status-copy">Waiting for state…</div>
      |        </section>
      |        <section>
      |          <h2>Races</h2>
      |          <div class="status-card race-board" id="race-board"></div>
      |        </section>
      |        <section>
      |          <h2>Events</h2>
      |          <div class="event-log" id="event-log"></div>
      |        </section>
      |      </aside>
      |    </div>
      |    <script src="https://unpkg.com/three@0.164.1/build/three.min.js"></script>
      |    <script src="https://unpkg.com/3d-force-graph@1.73.4/dist/3d-force-graph.min.js"></script>
      |    <script src="/app.js" defer></script>
      |  </body>
      |</html>
      |""".stripMargin

  private def appJs(): String =
    """const palette = [0x6ab8ff, 0xff8b5a, 0x59e0c4, 0xffd15c, 0xb490ff, 0xff6ab8, 0x90ef8b, 0xff4e50, 0x6effe6, 0xf7a072, 0x94d2bd, 0xffadad];
      |const UNCOLORED = 0x4a5a6c;
      |const CONFLICT  = 0xff5a8b;
      |const PULSE_MS  = 1400;
      |const POLL_MS   = 650;
      |const MAX_CONQUESTS = 3;
      |
      |const nodeMap = new Map();
      |const linkMap = new Map();
      |let topologySig = '';
      |let graph = null;
      |let autoRunTimer = null;
      |let autoBurstTimer = null;
      |let suppressAutoFit = false;
      |let currentMode = 'coloring';
      |
      |function linkKey(a, b) { return a < b ? `${a}::${b}` : `${b}::${a}`; }
      |function toHex(value) { return '#' + (value >>> 0).toString(16).padStart(6, '0'); }
      |
      |function blend(a, b, t) {
      |  const ar = (a >> 16) & 0xff, ag = (a >> 8) & 0xff, ab = a & 0xff;
      |  const br = (b >> 16) & 0xff, bg = (b >> 8) & 0xff, bb = b & 0xff;
      |  const r = Math.round(ar + (br - ar) * t);
      |  const g = Math.round(ag + (bg - ag) * t);
      |  const bl = Math.round(ab + (bb - ab) * t);
      |  return (r << 16) | (g << 8) | bl;
      |}
      |
      |function baseColor(n) {
      |  if (n.conflict && currentMode !== 'battle') return CONFLICT;
      |  if (n.color == null) return UNCOLORED;
      |  return palette[n.color % palette.length];
      |}
      |
      |function isExhausted(n) {
      |  return currentMode === 'battle' && (n.conquests != null) && n.conquests >= MAX_CONQUESTS;
      |}
      |
      |function visualColor(n) {
      |  if (n.__pulse && n.__pulse > 0) {
      |    return blend(baseColor(n), 0xffffff, Math.min(1, n.__pulse));
      |  }
      |  if (isExhausted(n)) {
      |    return blend(baseColor(n), 0x000000, 0.35);
      |  }
      |  return baseColor(n);
      |}
      |
      |function ensureGraph() {
      |  if (graph) return graph;
      |  if (typeof ForceGraph3D !== 'function') {
      |    console.error('3d-force-graph failed to load.');
      |    return null;
      |  }
      |  const mount = document.getElementById('graph');
      |  graph = ForceGraph3D()(mount)
      |    .backgroundColor('#03060c')
      |    .showNavInfo(false)
      |    .nodeRelSize(3)
      |    .nodeOpacity(1.0)
      |    .nodeResolution(12)
      |    .linkOpacity(0.32)
      |    .linkWidth(link => link.__bridge ? 1.8 : 0.5)
      |    .linkColor(link => link.__bridge ? '#ffd15c' : '#3b475a')
      |    .linkDirectionalParticles(link => link.__hot ? 5 : (link.__bridge ? 2 : 0))
      |    .linkDirectionalParticleWidth(link => link.__bridge ? 2.0 : 1.6)
      |    .linkDirectionalParticleSpeed(link => link.__bridge ? 0.008 : 0.012)
      |    .linkDirectionalParticleColor(link => link.__bridge ? '#ffe69c' : '#ffd15c')
      |    .nodeLabel(n => {
      |      const clr = n.color == null ? '-' : 'c' + n.color;
      |      const cid = n.clusterId != null ? ' · k' + n.clusterId : '';
      |      let conq = '';
      |      if (currentMode === 'battle' && n.conquests != null) {
      |        conq = n.conquests >= MAX_CONQUESTS ? ' · locked' : (n.conquests > 0 ? ` · ${n.conquests}/${MAX_CONQUESTS} wounds` : '');
      |      }
      |      return '<div style="font-family: ui-monospace, monospace; font-size: 12px; padding: 4px 8px; background: rgba(5,7,13,0.85); border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; color: #e8eef5;"><strong>' + n.id + '</strong> · ' + clr + cid + conq + (n.conflict ? ' · conflict' : '') + '</div>';
      |    })
      |    .nodeColor(n => toHex(visualColor(n)))
      |    .nodeVal(n => {
      |      const base = n.conflict ? 5 : 2.2;
      |      const locked = isExhausted(n) ? 1.4 : 0;
      |      const pulse = n.__pulse ? 26 * n.__pulse : 0;
      |      return base + locked + pulse;
      |    })
      |    .d3AlphaDecay(0.035)
      |    .d3VelocityDecay(0.32)
      |    .cooldownTicks(220)
      |    .warmupTicks(40);
      |
      |  const shell = document.getElementById('graph-shell');
      |  const sizeGraph = () => graph.width(shell.clientWidth).height(shell.clientHeight);
      |  sizeGraph();
      |  window.addEventListener('resize', sizeGraph);
      |
      |  const animatePulses = () => {
      |    const now = performance.now();
      |    let anyActive = false;
      |    nodeMap.forEach(n => {
      |      if (n.__spawnAt != null) {
      |        const t = (now - n.__spawnAt) / PULSE_MS;
      |        if (t >= 1) {
      |          delete n.__spawnAt;
      |          delete n.__pulse;
      |        } else {
      |          n.__pulse = 1 - t * t;
      |          anyActive = true;
      |        }
      |      }
      |    });
      |    let linksDirty = false;
      |    linkMap.forEach(link => {
      |      if (link.__hotUntil != null && link.__hotUntil < now) {
      |        link.__hot = false;
      |        delete link.__hotUntil;
      |        linksDirty = true;
      |      }
      |    });
      |    if (anyActive || linksDirty) graph.refresh();
      |    requestAnimationFrame(animatePulses);
      |  };
      |  requestAnimationFrame(animatePulses);
      |
      |  return graph;
      |}
      |
      |async function callPost(path) {
      |  const res = await fetch(path, { method: 'POST' });
      |  return res.json();
      |}
      |
      |async function fetchState() {
      |  const res = await fetch('/api/state');
      |  return res.json();
      |}
      |
      |function applyState(state) {
      |  if (!ensureGraph()) return;
      |  currentMode = state.mode || 'coloring';
      |  document.body.classList.toggle('mode-battle', currentMode === 'battle');
      |  const modeChip = document.getElementById('mode-chip');
      |  if (modeChip) {
      |    modeChip.textContent = currentMode;
      |    modeChip.classList.toggle('battle', currentMode === 'battle');
      |  }
      |  const battleBtn = document.getElementById('battle-btn');
      |  if (battleBtn) {
      |    const isBattle = currentMode === 'battle';
      |    battleBtn.setAttribute('aria-pressed', isBattle ? 'true' : 'false');
      |    battleBtn.textContent = isBattle ? 'Stop Battle' : 'Start Battle';
      |  }
      |  const incomingIds = new Set(state.nodes.map(n => n.id));
      |
      |  [...nodeMap.keys()].forEach(id => {
      |    if (!incomingIds.has(id)) nodeMap.delete(id);
      |  });
      |
      |  const freshNodes = [];
      |  state.nodes.forEach(remote => {
      |    let node = nodeMap.get(remote.id);
      |    if (!node) {
      |      node = { id: remote.id, __spawnAt: performance.now() };
      |      if (nodeMap.size > 0) {
      |        const anchorList = [...nodeMap.values()];
      |        const anchor = anchorList[Math.floor(Math.random() * anchorList.length)];
      |        node.x = (anchor.x || 0) + (Math.random() - 0.5) * 40;
      |        node.y = (anchor.y || 0) + (Math.random() - 0.5) * 40;
      |        node.z = (anchor.z || 0) + (Math.random() - 0.5) * 40;
      |      }
      |      nodeMap.set(remote.id, node);
      |      freshNodes.push(node);
      |    }
      |    const prevColor = node.color;
      |    node.color = remote.color;
      |    node.proposedColor = remote.proposedColor;
      |    node.conflict = remote.conflict;
      |    node.status = remote.status;
      |    node.neighbors = remote.neighbors;
      |    node.clusterId = remote.clusterId != null ? remote.clusterId : 0;
      |    node.conquests = remote.conquests != null ? remote.conquests : 0;
      |    if (state.mode === 'battle' && prevColor != null && prevColor !== node.color) {
      |      node.__spawnAt = performance.now();
      |    }
      |  });
      |
      |  const wantedKeys = new Set();
      |  state.nodes.forEach(n => n.neighbors.forEach(m => {
      |    if (incomingIds.has(m) && n.id < m) wantedKeys.add(linkKey(n.id, m));
      |  }));
      |
      |  [...linkMap.keys()].forEach(k => { if (!wantedKeys.has(k)) linkMap.delete(k); });
      |
      |  const addedLinks = [];
      |  const now = performance.now();
      |  wantedKeys.forEach(k => {
      |    const sep = k.indexOf('::');
      |    const a = k.substring(0, sep);
      |    const b = k.substring(sep + 2);
      |    let link = linkMap.get(k);
      |    if (!link) {
      |      link = { source: nodeMap.get(a), target: nodeMap.get(b), __hot: true, __hotUntil: now + 1700 };
      |      linkMap.set(k, link);
      |      addedLinks.push(link);
      |    }
      |    const srcNode = nodeMap.get(a);
      |    const tgtNode = nodeMap.get(b);
      |    link.__bridge = srcNode && tgtNode && srcNode.clusterId !== tgtNode.clusterId;
      |  });
      |
      |  const sig = nodeMap.size + '#' + wantedKeys.size;
      |  if (sig !== topologySig) {
      |    topologySig = sig;
      |    graph.graphData({ nodes: [...nodeMap.values()], links: [...linkMap.values()] });
      |    if (freshNodes.length > 0 && !suppressAutoFit) {
      |      setTimeout(() => graph.zoomToFit(700, 60), 900);
      |    }
      |  } else {
      |    graph.refresh();
      |  }
      |
      |  renderStats(state);
      |  renderEvents(state);
      |  renderLegend(state);
      |  renderRaces(state);
      |}
      |
      |function renderStats(state) {
      |  document.getElementById('graph-id').textContent = state.graphId;
      |  document.getElementById('round-value').textContent = state.round;
      |  document.getElementById('colored-value').textContent = state.nodes.filter(n => n.color != null).length;
      |  document.getElementById('conflicts-value').textContent = state.nodes.filter(n => n.conflict).length;
      |  document.getElementById('palette-value').textContent = state.paletteSize;
      |  document.getElementById('nodes-value').textContent = state.nodeCount;
      |  document.getElementById('clusters-value').textContent = (state.clusters || []).length;
      |  const lockedCount = state.mode === 'battle'
      |    ? state.nodes.filter(n => (n.conquests || 0) >= MAX_CONQUESTS).length
      |    : 0;
      |  document.getElementById('locked-value').textContent = lockedCount;
      |  const nodesInput = document.getElementById('nodes-input');
      |  const colorsInput = document.getElementById('colors-input');
      |  if (document.activeElement !== nodesInput) nodesInput.value = state.nodeCount;
      |  if (document.activeElement !== colorsInput) colorsInput.value = state.paletteSize;
      |  let status;
      |  if (state.mode === 'battle') {
      |    if (state.completed) {
      |      const victor = state.victor != null ? ('c' + state.victor) : 'the void';
      |      status = `Conquest complete after ${state.round} rounds - ${victor} reigns.`;
      |    } else if (state.running) {
      |      status = `Battle in progress · round ${state.round}. Survive by having ≥2 same-color neighbors.`;
      |    } else {
      |      status = `Battle paused at round ${state.round}. Step manually or resume Auto Run.`;
      |    }
      |  } else {
      |    status = state.completed
      |      ? `Colored in ${state.round} rounds with ${state.nodeCount} processes.`
      |      : state.running
      |        ? 'Auto-run active. Drag to orbit · wheel to zoom · right-click to pan.'
      |        : 'Paused. Hit Step, Auto Run, Spawn Cluster, or Start Battle.';
      |  }
      |  document.getElementById('status-copy').textContent = status;
      |}
      |
      |function renderRaces(state) {
      |  const root = document.getElementById('race-board');
      |  const races = (state.races || []).slice().sort((a, b) => b.size - a.size || a.color - b.color);
      |  if (races.length === 0) {
      |    root.innerHTML = '<div style="color: var(--muted); font-size: 0.8rem;">No colored processes yet.</div>';
      |    return;
      |  }
      |  const total = races.reduce((acc, r) => acc + r.size, 0) || 1;
      |  const top = races[0] ? races[0].size : 0;
      |  root.innerHTML = races.map(race => {
      |    const color = toHex(palette[race.color % palette.length]);
      |    const pct = Math.round((race.size / total) * 100);
      |    const width = Math.max(2, (race.size / Math.max(top, 1)) * 100);
      |    const isWinner = state.mode === 'battle' && state.completed && state.victor === race.color;
      |    return `<div class="race-row${isWinner ? ' winner' : ''}">
      |      <span class="swatch" style="background: ${color}"></span>
      |      <div class="bar"><div class="bar-fill" style="width: ${width}%; background: ${color}"></div></div>
      |      <span class="count">c${race.color} · ${race.size} · ${pct}%</span>
      |    </div>`;
      |  }).join('');
      |}
      |
      |function renderEvents(state) {
      |  const root = document.getElementById('event-log');
      |  root.innerHTML = state.events.slice().reverse().map(evt => {
      |    const target = evt.nodeId ? `<span class="target">${evt.nodeId}</span>` : '';
      |    return `<div class="event" data-kind="${evt.kind}"><strong>${evt.kind}</strong>${target}<div class="detail">${evt.detail}</div></div>`;
      |  }).join('');
      |}
      |
      |function renderLegend(state) {
      |  const root = document.getElementById('legend');
      |  const cells = [];
      |  for (let i = 0; i < state.paletteSize; i++) {
      |    cells.push(`<span><span class="dot" style="background: ${toHex(palette[i % palette.length])}"></span>c${i}</span>`);
      |  }
      |  cells.push(`<span><span class="dot" style="background: ${toHex(UNCOLORED)}"></span>uncolored</span>`);
      |  cells.push(`<span><span class="dot" style="background: ${toHex(CONFLICT)}"></span>conflict</span>`);
      |  cells.push('<span><span class="dot" style="background: #ffd15c"></span>cluster bridge</span>');
      |  if (state.mode === 'battle') {
      |    cells.push('<span><span class="dot" style="background: #3a3a3a; border: 1px solid #888;"></span>locked (' + MAX_CONQUESTS + '×)</span>');
      |  }
      |  root.innerHTML = cells.join('');
      |}
      |
      |async function refresh() {
      |  applyState(await fetchState());
      |}
      |
      |function startAutoRun() {
      |  stopAutoRun();
      |  autoRunTimer = setInterval(async () => {
      |    const state = await callPost('/api/step');
      |    applyState(state);
      |    const shouldStop = state.completed && (state.mode === 'battle' || autoBurstTimer == null);
      |    if (shouldStop) stopAutoRun();
      |  }, POLL_MS);
      |}
      |
      |function stopAutoRun() {
      |  if (autoRunTimer) clearInterval(autoRunTimer);
      |  autoRunTimer = null;
      |}
      |
      |function scheduleAutoBurst() {
      |  const delay = 1400 + Math.random() * 1800;
      |  autoBurstTimer = setTimeout(async () => {
      |    const size = 5 + Math.floor(Math.random() * 14);
      |    const bridges = 1 + Math.floor(Math.random() * 3);
      |    suppressAutoFit = true;
      |    try {
      |      applyState(await callPost(`/api/burst?size=${size}&bridges=${bridges}`));
      |    } finally {
      |      suppressAutoFit = false;
      |    }
      |    if (autoBurstTimer != null) scheduleAutoBurst();
      |  }, delay);
      |}
      |
      |function toggleAutoBurst() {
      |  const btn = document.getElementById('auto-burst-btn');
      |  const isOn = btn.getAttribute('aria-pressed') === 'true';
      |  if (isOn) {
      |    btn.setAttribute('aria-pressed', 'false');
      |    if (autoBurstTimer) clearTimeout(autoBurstTimer);
      |    autoBurstTimer = null;
      |  } else {
      |    btn.setAttribute('aria-pressed', 'true');
      |    scheduleAutoBurst();
      |    if (!autoRunTimer) startAutoRun();
      |  }
      |}
      |
      |function hardReset() {
      |  nodeMap.clear();
      |  linkMap.clear();
      |  topologySig = '';
      |}
      |
      |document.getElementById('start-btn').addEventListener('click', async () => {
      |  applyState(await callPost('/api/start'));
      |  startAutoRun();
      |});
      |document.getElementById('pause-btn').addEventListener('click', async () => {
      |  stopAutoRun();
      |  applyState(await callPost('/api/pause'));
      |});
      |document.getElementById('step-btn').addEventListener('click', async () => {
      |  stopAutoRun();
      |  applyState(await callPost('/api/step'));
      |});
      |document.getElementById('reset-btn').addEventListener('click', async () => {
      |  stopAutoRun();
      |  hardReset();
      |  applyState(await callPost('/api/reset'));
      |});
      |document.getElementById('apply-btn').addEventListener('click', async () => {
      |  stopAutoRun();
      |  hardReset();
      |  const nodes = document.getElementById('nodes-input').value;
      |  const colors = document.getElementById('colors-input').value;
      |  applyState(await callPost(`/api/configure?nodes=${encodeURIComponent(nodes)}&colors=${encodeURIComponent(colors)}`));
      |});
      |document.getElementById('burst-btn').addEventListener('click', async () => {
      |  const size = document.getElementById('burst-input').value || 8;
      |  const bridges = document.getElementById('bridges-input').value || 1;
      |  applyState(await callPost(`/api/burst?size=${encodeURIComponent(size)}&bridges=${encodeURIComponent(bridges)}`));
      |});
      |document.getElementById('auto-burst-btn').addEventListener('click', toggleAutoBurst);
      |document.getElementById('battle-btn').addEventListener('click', async () => {
      |  const isBattle = currentMode === 'battle';
      |  stopAutoRun();
      |  const endpoint = isBattle ? '/api/battle/stop' : '/api/battle/start';
      |  applyState(await callPost(endpoint));
      |  if (!isBattle) startAutoRun();
      |});
      |
      |refresh();
      |""".stripMargin

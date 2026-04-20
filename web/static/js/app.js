/* ============================================================
   Asset Ledger — frontend
   ============================================================ */

const LWC = window.LightweightCharts;

const STATE = {
    symbol: null,
    tf: "1D",
    type: "candles",
    tz: "UTC",                      // "UTC" or "ET" — affects chart axis + crosshair + HUD
    indicators: new Set(),          // e.g. "sma20", "ema12", "vwap", "bb20", "rsi14", "macd", "stoch14"
    events: new Set(),              // "earnings", "dividends", "splits"
    eventData: null,                // cached /events response
    bars: null,                     // last fetched /bars response
    chart: null,
    priceSeries: null,
    volumeSeries: null,
    indSeries: {},                  // ind key -> line series (inchart only)
    subpanes: {},                   // { rsi: {chart, series...}, macd: {...}, stoch: {...} }
    assets: [],
    raw: { page: 0, page_size: 100, order: "desc" },
    syncGuard: false,               // prevents infinite recursion in timescale sync
    crosshairGuard: false,          // prevents infinite recursion in crosshair sync
};

// Classify which indicators render inchart vs in a separate pane
const SUBPANE_INDICATORS = {
    rsi14:   "rsi",
    macd:    "macd",
    stoch14: "stoch",
};

// ---------- timezone-aware formatters --------------------------------------
function tzOpts() {
    return STATE.tz === "ET" ? { timeZone: "America/New_York" } : { timeZone: "UTC" };
}
function tzLabel() { return STATE.tz; }

/** Axis tick formatter. `time` is unix seconds, a date string, or a BusinessDay obj. */
function axisTickFormatter(time, tickMarkType /*, locale*/) {
    // Daily data: LWC converts "YYYY-MM-DD" strings to BusinessDay { year, month, day }.
    if (typeof time === "object" && time && "year" in time) {
        const d = new Date(Date.UTC(time.year, time.month - 1, time.day));
        // month/year ticks for daily
        if (tickMarkType === 0) return String(time.year);
        if (tickMarkType === 1) return new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric", timeZone: "UTC" }).format(d);
        return new Intl.DateTimeFormat("en-US", { month: "short", day: "2-digit", timeZone: "UTC" }).format(d);
    }
    if (typeof time === "string") return time;  // defensive
    const d = new Date(time * 1000);
    const opts = tzOpts();
    switch (tickMarkType) {
        case 0: return new Intl.DateTimeFormat("en-US", { year: "numeric", ...opts }).format(d);
        case 1: return new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric", ...opts }).format(d);
        case 2: return new Intl.DateTimeFormat("en-US", { month: "short", day: "2-digit", ...opts }).format(d);
        case 3: return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hour12: false, ...opts }).format(d);
        case 4: return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, ...opts }).format(d);
        default: return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hour12: false, ...opts }).format(d);
    }
}

/** Crosshair label formatter (the tooltip on the time axis). */
function crosshairTimeFormatter(time) {
    if (typeof time === "string") return time;
    if (typeof time === "object" && time && time.year) {
        return `${time.year}-${String(time.month).padStart(2,"0")}-${String(time.day).padStart(2,"0")}`;
    }
    const d = new Date(time * 1000);
    const opts = { year: "numeric", month: "2-digit", day: "2-digit",
                   hour: "2-digit", minute: "2-digit", hour12: false, ...tzOpts() };
    // Intl returns "04/17/2026, 15:59" — reformat to "2026-04-17 15:59"
    const parts = new Intl.DateTimeFormat("en-US", opts).formatToParts(d);
    const g = (t) => parts.find(p => p.type === t)?.value;
    return `${g("year")}-${g("month")}-${g("day")} ${g("hour")}:${g("minute")}`;
}

/** HUD time readout (top-left of chart). */
function hudTimeString(t) {
    if (typeof t === "string") return t;
    if (typeof t === "object" && t && t.year) {
        return `${t.year}-${String(t.month).padStart(2,"0")}-${String(t.day).padStart(2,"0")}`;
    }
    if (typeof t === "number") {
        return crosshairTimeFormatter(t) + " " + tzLabel();
    }
    return "—";
}

/** Re-applies timezone-sensitive chart options. Called on tz toggle. */
function applyTimezoneToChart() {
    if (!STATE.chart) return;
    STATE.chart.applyOptions({
        timeScale: { tickMarkFormatter: axisTickFormatter },
        localization: { timeFormatter: crosshairTimeFormatter },
    });
}

const INDICATOR_COLORS = {
    sma20:  "#f0f0f0",
    sma50:  "#c0c0c0",
    sma200: "#888",
    ema12:  "#d0d0d0",
    ema26:  "#9a9a9a",
    vwap:   "#ffffff",
    bb20_mid:   "#888",
    bb20_upper: "#555",
    bb20_lower: "#555",
};

// ---------- helpers ---------------------------------------------------------
const $ = (id) => document.getElementById(id);
const fmtPrice = (v, decimals = 2) => v == null ? "—" :
    Number(v).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
const fmtInt = (v) => v == null ? "—" : Number(v).toLocaleString("en-US");
const fmtPct = (v, dp = 2) => v == null ? "—" : (v >= 0 ? "+" : "") + v.toFixed(dp) + "%";
const fmtSigned = (v, dp = 2) => v == null ? "—" : (v >= 0 ? "+" : "") + fmtPrice(v, dp);

function signClass(v) {
    if (v == null) return "flat";
    if (v > 0) return "up";
    if (v < 0) return "down";
    return "flat";
}

function fmtLargeNum(v) {
    if (v == null) return "—";
    const a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(2) + "T";
    if (a >= 1e9)  return (v / 1e9).toFixed(2)  + "B";
    if (a >= 1e6)  return (v / 1e6).toFixed(2)  + "M";
    if (a >= 1e3)  return (v / 1e3).toFixed(2)  + "K";
    return v.toLocaleString("en-US");
}

async function api(path) {
    const r = await fetch(path);
    if (!r.ok) throw new Error(`${path} → ${r.status}`);
    return r.json();
}

// ---------- clock ----------------------------------------------------------
function tick() {
    const now = new Date();
    const utc = now.toISOString().slice(11, 19);
    const et = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        hour: "2-digit", minute: "2-digit", second: "2-digit",
        hour12: false,
    }).format(now);
    $("hdr-utc").textContent = utc;
    $("hdr-et").textContent  = et;
    $("status-clock").textContent = utc + " UTC";
}
setInterval(tick, 1000); tick();

// ---------- sidebar --------------------------------------------------------
async function loadAssets() {
    const [ov, assets, universe] = await Promise.all([
        api("/api/overview"),
        api("/api/assets"),
        api("/api/universe").catch(() => ({ loaded: false, tiers: {}, rows: [] })),
    ]);
    STATE.assets = assets;
    STATE.universe = universe;
    $("hdr-db").textContent = fmtInt(ov.n_bars_total) + " bars";
    $("side-count").textContent = assets.length;

    renderProgress(universe);
    renderAssetList();

    const search = $("side-search");
    if (search && !search._wired) {
        search.addEventListener("input", renderAssetList);
        search._wired = true;
    }

    if (assets.length && !STATE.symbol) selectAsset(assets[0].symbol);
}

function renderProgress(u) {
    const box = $("side-progress");
    if (!box) return;
    if (!u || !u.loaded) { box.innerHTML = ""; return; }
    const rows = [];
    for (const tier of ["sp500", "sp400", "sp600"]) {
        const t = u.tiers[tier] || { done: 0, total: 0 };
        const pct = t.total ? Math.round(t.done / t.total * 100) : 0;
        const label = tier.replace("sp", "S&P ");
        rows.push(`
            <div class="prog-row">
                <span class="prog-label">${label}</span>
                <span class="prog-count">${t.done}/${t.total}</span>
                <div class="prog-bar"><div class="prog-fill" style="width:${pct}%"></div></div>
            </div>`);
    }
    box.innerHTML = rows.join("");
}

function renderAssetList() {
    const list = $("asset-list");
    const search = ($("side-search")?.value || "").trim().toUpperCase();
    list.innerHTML = "";

    const assetsBySym = new Map(STATE.assets.map(a => [a.symbol, a]));
    const universeRows = (STATE.universe && STATE.universe.rows) || [];

    // Ordering: ingested first (alphabetical), then non-ingested universe (alphabetical).
    const ingestedSyms = new Set(STATE.assets.map(a => a.symbol));
    const ingestedSorted = [...STATE.assets].sort((a, b) => a.symbol.localeCompare(b.symbol));
    const pending = universeRows
        .filter(r => !ingestedSyms.has(r.symbol))
        .sort((a, b) => a.symbol.localeCompare(b.symbol));

    const matchesSearch = (sym, name) => {
        if (!search) return true;
        return sym.includes(search) || (name || "").toUpperCase().includes(search);
    };

    // Ingested rows — full price/change info
    for (const a of ingestedSorted) {
        if (!matchesSearch(a.symbol, "")) continue;
        const row = document.createElement("div");
        row.className = "asset-row";
        row.dataset.symbol = a.symbol;
        const cls = signClass(a.change);
        row.innerHTML = `
            <div class="sym">${a.symbol}</div>
            <div class="price num">${fmtPrice(a.last_price)}</div>
            <div class="sub">
                <span>${fmtSigned(a.change)}</span>
                <span class="chg num ${cls}">${fmtPct(a.change_pct)}</span>
            </div>`;
        row.addEventListener("click", () => selectAsset(a.symbol));
        list.appendChild(row);
    }

    // Pending rows (S&P 1500 not yet ingested) — greyed, not clickable
    for (const r of pending) {
        if (!matchesSearch(r.symbol, r.name)) continue;
        const row = document.createElement("div");
        row.className = "asset-row pending";
        row.dataset.symbol = r.symbol;
        row.title = `${r.name} · ${r.tier}${r.sector ? " · " + r.sector : ""} — not ingested yet`;
        row.innerHTML = `
            <div class="sym">${r.symbol}</div>
            <div class="price num">—</div>
            <div class="sub"><span class="tier-tag">${r.tier}</span></div>`;
        list.appendChild(row);
    }

    highlightActive();
}

function highlightActive() {
    document.querySelectorAll(".asset-row").forEach(r => {
        r.classList.toggle("active", r.dataset.symbol === STATE.symbol);
    });
}

async function selectAsset(symbol) {
    STATE.symbol = symbol;
    highlightActive();
    $("status-sym").textContent = symbol;
    const a = STATE.assets.find(x => x.symbol === symbol);
    if (a) {
        $("hdr-sym").textContent   = symbol;
        $("hdr-price").textContent = fmtPrice(a.last_price);
        $("hdr-chg").textContent   = `${fmtSigned(a.change)}  ${fmtPct(a.change_pct)}`;
        $("hdr-chg").className     = "chg num " + signClass(a.change);
    }
    await loadBars();
    await loadActiveTab();
}

// ---------- chart ----------------------------------------------------------
function createChart() {
    const el = $("chart");
    const chart = LWC.createChart(el, {
        width:  el.clientWidth  || 1000,
        height: el.clientHeight || 500,
        layout: {
            background: { color: "#000" },
            textColor: "#b8b8b8",
            fontFamily: '"Plex Mono", "Andale Mono", Menlo, monospace',
            fontSize: 11,
            attributionLogo: false,
        },
        grid: {
            vertLines: { color: "#141414" },
            horzLines: { color: "#141414" },
        },
        rightPriceScale: {
            borderColor: "#222",
            scaleMargins: { top: 0.05, bottom: 0.28 },
        },
        timeScale: {
            borderColor: "#222",
            timeVisible: true,
            secondsVisible: false,
            tickMarkFormatter: axisTickFormatter,
        },
        localization: {
            timeFormatter: crosshairTimeFormatter,
        },
        crosshair: {
            mode: LWC.CrosshairMode.Normal,
            vertLine: { color: "#444", width: 1, style: 2, labelBackgroundColor: "#1c1c1c" },
            horzLine: { color: "#444", width: 1, style: 2, labelBackgroundColor: "#1c1c1c" },
        },
    });

    const price = chart.addCandlestickSeries({
        upColor: "#30d158", downColor: "#ff3b30",
        wickUpColor: "#30d158", wickDownColor: "#ff3b30",
        borderUpColor: "#30d158", borderDownColor: "#ff3b30",
        priceLineColor: "#666",
        priceLineStyle: 2,
        priceLineWidth: 1,
    });
    const volume = chart.addHistogramSeries({
        priceFormat: { type: "volume" },
        priceScaleId: "vol",
        color: "#444",
    });
    chart.priceScale("vol").applyOptions({
        scaleMargins: { top: 0.78, bottom: 0 },
    });

    // resize
    new ResizeObserver(() => {
        chart.applyOptions({ width: el.clientWidth, height: el.clientHeight });
        positionEarningsOverlay();
    }).observe(el);

    // Reposition earnings overlay on every pan/zoom
    chart.timeScale().subscribeVisibleTimeRangeChange(() => positionEarningsOverlay());

    // crosshair HUD
    chart.subscribeCrosshairMove(param => {
        if (!param || !param.time || !STATE.bars) {
            return;
        }
        const d = param.seriesData.get(STATE.priceSeries);
        const v = param.seriesData.get(STATE.volumeSeries);
        const vwap = STATE.indSeries.vwap && param.seriesData.get(STATE.indSeries.vwap);
        if (d && typeof d === "object" && "open" in d) {
            $("hud-o").textContent = fmtPrice(d.open);
            $("hud-h").textContent = fmtPrice(d.high);
            $("hud-l").textContent = fmtPrice(d.low);
            $("hud-c").textContent = fmtPrice(d.close);
            const ch = d.close - d.open;
            const chp = ch / d.open * 100;
            const el = $("hud-chg");
            el.textContent = `${fmtSigned(ch)}  ${fmtPct(chp)}`;
            el.className = "v " + signClass(ch);
        } else if (d && typeof d === "object" && "value" in d) {
            ["hud-o","hud-h","hud-l"].forEach(i => $(i).textContent = "—");
            $("hud-c").textContent = fmtPrice(d.value);
            $("hud-chg").textContent = "—";
        }
        if (v) $("hud-v").textContent = fmtLargeNum(v.value);
        if (vwap) $("hud-vwap").textContent = fmtPrice(vwap.value);
        // time (tz-aware via hudTimeString)
        $("hud-t").textContent = hudTimeString(param.time);
    });

    STATE.chart = chart;
    STATE.priceSeries = price;
    STATE.volumeSeries = volume;
    installTimescaleSync(chart);
    installCrosshairSync(chart, "price");
}

async function loadBars() {
    if (!STATE.symbol) return;
    const indList = Array.from(STATE.indicators).join(",");
    const url = `/api/assets/${STATE.symbol}/bars?tf=${STATE.tf}&limit=5000${indList ? "&indicators=" + indList : ""}`;
    const data = await api(url);
    STATE.bars = data;

    $("status-tf").textContent = STATE.tf;
    $("status-bars").textContent = fmtInt(data.n) + " bars";

    if (!data.candles.length) {
        STATE.priceSeries.setData([]);
        STATE.volumeSeries.setData([]);
        return;
    }

    if (STATE.type === "candles") {
        STATE.priceSeries.setData(data.candles);
    } else {
        // line series: map candles -> {time, value} using close
        STATE.priceSeries.setData(
            data.candles.map(c => ({ time: c.time, value: c.close }))
        );
    }
    STATE.volumeSeries.setData(data.volume);

    // ---- inchart indicators (overlay on price pane) ----
    // Skip keys that belong in subpanes (rsi*, macd_*, stoch*).
    const isSubpaneKey = k => /^(rsi|macd|stoch)/.test(k);
    for (const k of Object.keys(STATE.indSeries)) {
        if (!(k in data.indicators) || isSubpaneKey(k)) {
            STATE.chart.removeSeries(STATE.indSeries[k]);
            delete STATE.indSeries[k];
        }
    }
    for (const [k, series] of Object.entries(data.indicators)) {
        if (!series.length || isSubpaneKey(k)) continue;
        if (!(k in STATE.indSeries)) {
            const col = INDICATOR_COLORS[k] || "#bfbfbf";
            STATE.indSeries[k] = STATE.chart.addLineSeries({
                color: col,
                lineWidth: 1,
                lineStyle: k.includes("lower") || k.includes("upper") ? 2 : 0,
                priceLineVisible: false,
                lastValueVisible: false,
            });
        }
        STATE.indSeries[k].setData(series);
    }

    // ---- subpane indicators ----
    if (STATE.subpanes.rsi && data.indicators.rsi14) {
        STATE.subpanes.rsi.series.rsi.setData(data.indicators.rsi14);
    }
    if (STATE.subpanes.macd && data.indicators.macd_line) {
        STATE.subpanes.macd.series.line.setData(data.indicators.macd_line);
        STATE.subpanes.macd.series.signal.setData(data.indicators.macd_signal || []);
        // Histogram coloring: green for positive, red for negative
        const histData = (data.indicators.macd_hist || []).map(p => ({
            time: p.time, value: p.value,
            color: p.value >= 0 ? "#30d15888" : "#ff3b3088",
        }));
        STATE.subpanes.macd.series.hist.setData(histData);
    }
    if (STATE.subpanes.stoch && data.indicators.stoch14_k) {
        STATE.subpanes.stoch.series.k.setData(data.indicators.stoch14_k);
        STATE.subpanes.stoch.series.d.setData(data.indicators.stoch14_d || []);
    }
    renderLegend();
    STATE.chart.timeScale().fitContent();

    // Fetch events if any toggle is on, then apply
    if (STATE.events.size > 0) {
        try { await ensureEventData(); applyEventMarkers(); } catch (e) { /* ignore */ }
    } else if (STATE.priceSeries && STATE.priceSeries.setMarkers) {
        STATE.priceSeries.setMarkers([]);
    }

    // Auto-disable event toggles for which no events fall in the loaded window
    await updateEventButtonStates();
}

function renderLegend() {
    const el = $("legend");
    el.innerHTML = "";
    for (const k of Object.keys(STATE.indSeries)) {
        const color = INDICATOR_COLORS[k] || "#bfbfbf";
        const div = document.createElement("div");
        div.className = "ind";
        div.innerHTML = `<span class="sw" style="background:${color}"></span>${k.toUpperCase()}`;
        el.appendChild(div);
    }
}

// ---------- multi-pane chart layout ----------------------------------------
// OpenBB's pattern (openbb_charting/core/plotly_ta/ta_class.py::get_fig_settings_dict)
// — lookup table of pane heights by subpane count. Price pane always dominates.
const PANE_HEIGHTS = {
    0: [1],
    1: [0.70, 0.30],
    2: [0.60, 0.20, 0.20],
    3: [0.55, 0.15, 0.15, 0.15],
};

function activeSubpanes() {
    const out = [];
    for (const [ind, kind] of Object.entries(SUBPANE_INDICATORS)) {
        if (STATE.indicators.has(ind)) out.push(kind);
    }
    return out;
}

function layoutPanes() {
    const subs = activeSubpanes();
    const heights = PANE_HEIGHTS[subs.length] || PANE_HEIGHTS[3];
    const panesEl = document.getElementById("panes");
    panesEl.style.gridTemplateRows = heights.map(h => `${h}fr`).join(" ");
}

function commonChartOptions() {
    return {
        layout: {
            background: { color: "#000" },
            textColor: "#b8b8b8",
            fontFamily: '"Plex Mono", "Andale Mono", Menlo, monospace',
            fontSize: 11,
            attributionLogo: false,
        },
        grid: {
            vertLines: { color: "#141414" },
            horzLines: { color: "#141414" },
        },
        rightPriceScale: { borderColor: "#222" },
        timeScale: {
            borderColor: "#222",
            timeVisible: true,
            secondsVisible: false,
            tickMarkFormatter: axisTickFormatter,
            visible: false,              // only main chart shows time axis
        },
        localization: { timeFormatter: crosshairTimeFormatter },
        crosshair: {
            mode: LWC.CrosshairMode.Normal,
            vertLine: { color: "#444", width: 1, style: 2, labelBackgroundColor: "#1c1c1c" },
            horzLine: { color: "#444", width: 1, style: 2, labelBackgroundColor: "#1c1c1c" },
        },
    };
}

function installTimescaleSync(chart) {
    // Install ONCE per chart at creation. Handler reads the current pane set at
    // invocation time, so new subpanes added later get kept in sync automatically.
    // Uses TIME-based sync (not logical index) because subpanes have warmup
    // trimming (RSI drops 14 bars, MACD ~26, etc.) — logical index 0 maps to
    // different timestamps across panes, which would misalign them.
    chart.timeScale().subscribeVisibleTimeRangeChange(range => {
        if (!range || STATE.syncGuard) return;
        STATE.syncGuard = true;
        try {
            const all = [STATE.chart, ...Object.values(STATE.subpanes).map(p => p.chart)];
            for (const dst of all) {
                if (dst && dst !== chart) {
                    try { dst.timeScale().setVisibleRange(range); } catch (e) {}
                }
            }
        } finally {
            // release on next microtask so re-entrant callbacks bail immediately
            queueMicrotask(() => { STATE.syncGuard = false; });
        }
    });
}

/**
 * Synchronize the vertical crosshair across all panes. When the user moves the
 * mouse over any pane, the other panes display a vertical line at the same
 * time index. Install ONCE per chart at creation.
 */
function installCrosshairSync(chart, kind /* "price" | "rsi" | "macd" | "stoch" */) {
    chart.subscribeCrosshairMove(param => {
        if (STATE.crosshairGuard) return;
        STATE.crosshairGuard = true;
        try {
            // Collect [chart, representative series] for every other pane
            const targets = [];
            if (kind !== "price" && STATE.chart && STATE.priceSeries) {
                targets.push({ chart: STATE.chart, series: STATE.priceSeries });
            }
            for (const [k, p] of Object.entries(STATE.subpanes)) {
                if (k === kind || !p || !p.chart) continue;
                // pick any series for that pane (needed by setCrosshairPosition API)
                const rep = p.series.rsi || p.series.line || p.series.k;
                if (rep) targets.push({ chart: p.chart, series: rep });
            }
            if (!param || !param.time) {
                for (const t of targets) {
                    try { t.chart.clearCrosshairPosition(); } catch (e) {}
                }
                return;
            }
            for (const t of targets) {
                try {
                    // price arg is irrelevant for the vertical line; pass 0.
                    t.chart.setCrosshairPosition(0, param.time, t.series);
                } catch (e) {}
            }
        } finally {
            queueMicrotask(() => { STATE.crosshairGuard = false; });
        }
    });
}

function createSubpane(kind) {
    // Create a DOM container appended to #panes
    const panesEl = document.getElementById("panes");
    const wrap = document.createElement("div");
    wrap.className = `pane pane-${kind}`;
    wrap.id = `pane-${kind}`;
    const label = document.createElement("div");
    label.className = "pane-label";
    label.innerHTML = ({
        rsi:   'RSI <span class="v" id="pane-rsi-val">—</span>',
        macd:  'MACD <span class="v" id="pane-macd-val">—</span>',
        stoch: 'STOCH <span class="v" id="pane-stoch-val">—</span>',
    })[kind];
    const chartDiv = document.createElement("div");
    chartDiv.className = "pane-chart";
    wrap.appendChild(label);
    wrap.appendChild(chartDiv);
    panesEl.appendChild(wrap);

    const chart = LWC.createChart(chartDiv, {
        ...commonChartOptions(),
        width: chartDiv.clientWidth || 1000,
        height: chartDiv.clientHeight || 150,
    });
    new ResizeObserver(() => chart.applyOptions({
        width: chartDiv.clientWidth, height: chartDiv.clientHeight,
    })).observe(chartDiv);

    const series = {};
    if (kind === "rsi") {
        series.rsi = chart.addLineSeries({ color: "#e0d87a", lineWidth: 1, priceLineVisible: false });
        // Fixed 0-100 scale via explicit price range later via setData; add 30/70 lines as horizontal series
        // Using price lines (legend-less overbought/oversold markers)
        series.rsi.createPriceLine({ price: 70, color: "#ff3b3055", lineStyle: 2, lineWidth: 1, axisLabelVisible: false });
        series.rsi.createPriceLine({ price: 30, color: "#30d15855", lineStyle: 2, lineWidth: 1, axisLabelVisible: false });
        series.rsi.createPriceLine({ price: 50, color: "#33333388", lineStyle: 3, lineWidth: 1, axisLabelVisible: false });
    } else if (kind === "macd") {
        // histogram first (bars), then the two lines on top
        series.hist   = chart.addHistogramSeries({ color: "#555", priceLineVisible: false, lastValueVisible: true });
        series.line   = chart.addLineSeries({ color: "#f0f0f0", lineWidth: 1, priceLineVisible: false });
        series.signal = chart.addLineSeries({ color: "#9a9a9a", lineWidth: 1, priceLineVisible: false });
        // zero line
        series.line.createPriceLine({ price: 0, color: "#33333388", lineStyle: 3, lineWidth: 1, axisLabelVisible: false });
    } else if (kind === "stoch") {
        series.k = chart.addLineSeries({ color: "#f0f0f0", lineWidth: 1, priceLineVisible: false });
        series.d = chart.addLineSeries({ color: "#9a9a9a", lineWidth: 1, priceLineVisible: false });
        series.k.createPriceLine({ price: 80, color: "#ff3b3055", lineStyle: 2, lineWidth: 1, axisLabelVisible: false });
        series.k.createPriceLine({ price: 20, color: "#30d15855", lineStyle: 2, lineWidth: 1, axisLabelVisible: false });
    }
    installTimescaleSync(chart);
    installCrosshairSync(chart, kind);
    // Immediately align this new pane's visible range with the main chart so it
    // doesn't fitContent to its own (warmup-trimmed) narrower range.
    try {
        const mainRange = STATE.chart?.timeScale().getVisibleRange();
        if (mainRange) chart.timeScale().setVisibleRange(mainRange);
    } catch (e) {}
    return { chart, series, kind };
}

function destroySubpane(kind) {
    const panel = STATE.subpanes[kind];
    if (!panel) return;
    try { panel.chart.remove(); } catch (e) {}
    const dom = document.getElementById(`pane-${kind}`);
    if (dom) dom.remove();
    delete STATE.subpanes[kind];
}

function updatePanes() {
    // Reconcile active subpanes vs STATE.subpanes
    const wanted = new Set(activeSubpanes());
    for (const kind of Object.keys(STATE.subpanes)) {
        if (!wanted.has(kind)) destroySubpane(kind);
    }
    for (const kind of wanted) {
        if (!STATE.subpanes[kind]) STATE.subpanes[kind] = createSubpane(kind);
    }
    layoutPanes();
    // Re-attach axis visibility: bottom-most pane shows the time axis
    const order = ["price", "rsi", "macd", "stoch"].filter(k => k === "price" || STATE.subpanes[k]);
    order.forEach((k, idx) => {
        const isLast = idx === order.length - 1;
        const chart = (k === "price") ? STATE.chart : STATE.subpanes[k].chart;
        chart.applyOptions({ timeScale: { visible: isLast, tickMarkFormatter: axisTickFormatter } });
    });
    // Sync handlers are installed per-chart at creation (see installTimescaleSync),
    // and they read the current pane set dynamically — no re-wiring needed.
}

// ---------- chart event markers --------------------------------------------
async function ensureEventData() {
    if (!STATE.symbol) return null;
    if (STATE.eventData && STATE.eventData._symbol === STATE.symbol) return STATE.eventData;
    const d = await api(`/api/assets/${STATE.symbol}/events`);
    d._symbol = STATE.symbol;
    STATE.eventData = d;
    return d;
}

async function updateEventButtonStates() {
    // Enable/disable the EARN/DIV/SPLIT buttons based on whether any events
    // of that type fall within the currently loaded bars.
    const buttons = {
        earnings:  document.querySelector('#evt-group button[data-evt="earnings"]'),
        dividends: document.querySelector('#evt-group button[data-evt="dividends"]'),
        splits:    document.querySelector('#evt-group button[data-evt="splits"]'),
    };

    const isDaily = STATE.tf === "1D" || STATE.tf === "1W";
    if (!isDaily) {
        for (const [kind, btn] of Object.entries(buttons)) {
            if (!btn) continue;
            btn.disabled = true;
            btn.title = `Event markers only render on 1D / 1W timeframes — switch timeframe to use.`;
            btn.classList.remove("active");
        }
        STATE.events.clear();
        return;
    }

    if (!STATE.bars || !STATE.bars.candles?.length) return;
    try {
        await ensureEventData();
    } catch (e) { return; }
    const ed = STATE.eventData || {};
    const barTimes = new Set(STATE.bars.candles.map(c => c.time));
    const firstDay = STATE.bars.candles[0].time;
    const lastDay  = STATE.bars.candles[STATE.bars.candles.length - 1].time;

    const countInRange = (arr, key = "ts_date") =>
        (arr || []).reduce((n, ev) => n + (barTimes.has(ev[key]) ? 1 : 0), 0);
    const totals = {
        earnings:  (ed.earnings  || []).length,
        dividends: (ed.dividends || []).length,
        splits:    (ed.splits    || []).length,
    };
    const inRange = {
        earnings:  countInRange(ed.earnings),
        dividends: countInRange(ed.dividends),
        splits:    countInRange(ed.splits),
    };

    for (const [kind, btn] of Object.entries(buttons)) {
        if (!btn) continue;
        const tot = totals[kind], hit = inRange[kind];
        if (tot === 0) {
            btn.disabled = true;
            btn.title = `No ${kind} data ingested.`;
        } else if (hit === 0) {
            btn.disabled = true;
            btn.title = `${tot} ${kind} on record — none fall in the loaded window (${firstDay} → ${lastDay}).`
                      + (kind === "splits" ? "  (MSFT's last split was 2003-02-18.)" : "");
            btn.classList.remove("active");
            STATE.events.delete(kind);
        } else {
            btn.disabled = false;
            btn.title = `${hit} ${kind} in window · ${tot} on record`;
        }
    }
}

function applyEventMarkers() {
    if (!STATE.priceSeries || !STATE.bars) return;
    if (!STATE.eventData) {
        STATE.priceSeries.setMarkers([]);
        renderEarningsOverlay([]);
        return;
    }

    const barTimes = new Set(STATE.bars.candles.map(c => c.time));
    const isDaily  = STATE.tf === "1D" || STATE.tf === "1W";
    const markers = [];

    // Earnings render via custom DOM overlay (badges + dashed vline + tooltip) —
    // keeps the setMarkers path for dividends/splits only.
    const earnings = [];
    if (STATE.events.has("earnings") && STATE.eventData.earnings && isDaily) {
        // Include both past (in-window) and scheduled (future) earnings.
        // Scheduled events always render even if past the last bar.
        for (const e of STATE.eventData.earnings) {
            const inWindow = barTimes.has(e.ts_date);
            const isScheduled = e.status === "scheduled";
            if (inWindow || isScheduled) earnings.push(e);
        }
    }
    renderEarningsOverlay(earnings);

    if (STATE.events.has("dividends") && STATE.eventData.dividends) {
        for (const d of STATE.eventData.dividends) {
            if (!isDaily) continue;
            if (!barTimes.has(d.ts_date)) continue;
            markers.push({
                time: d.ts_date,
                position: "belowBar",
                color: "#888",
                shape: "arrowUp",
                text: `D $${d.dividend_amount?.toFixed(2)}`,
            });
        }
    }
    if (STATE.events.has("splits") && STATE.eventData.splits) {
        for (const s of STATE.eventData.splits) {
            if (!isDaily) continue;
            if (!barTimes.has(s.ts_date)) continue;
            markers.push({
                time: s.ts_date,
                position: "belowBar",
                color: "#f0f0f0",
                shape: "square",
                text: `S ${s.split_ratio}:1`,
            });
        }
    }
    markers.sort((a, b) => a.time < b.time ? -1 : a.time > b.time ? 1 : 0);
    STATE.priceSeries.setMarkers(markers);
}

// ---------- earnings DOM overlay (badge strip + hover vline + tooltip) -----

/** Ensure a single `.evt-overlay` div exists inside `.pane-price` and return it. */
function ensureEventOverlay() {
    const pricePane = document.querySelector(".pane-price");
    if (!pricePane) return null;
    let ov = pricePane.querySelector(".evt-overlay");
    if (!ov) {
        ov = document.createElement("div");
        ov.className = "evt-overlay";
        pricePane.appendChild(ov);
        // tooltip singleton
        const tip = document.createElement("div");
        tip.className = "evt-tooltip";
        ov.appendChild(tip);
    }
    return ov;
}

let _earningsData = [];   // last-rendered earnings, for repositioning on zoom/pan

function renderEarningsOverlay(earnings) {
    const ov = ensureEventOverlay();
    if (!ov) return;
    _earningsData = earnings.slice();
    // Remove existing badges + vlines (keep the tooltip singleton)
    ov.querySelectorAll(".evt-badge, .evt-vline").forEach(n => n.remove());
    const tip = ov.querySelector(".evt-tooltip");
    tip.classList.remove("shown");

    if (!earnings.length || !STATE.chart) return;
    const ts = STATE.chart.timeScale();

    for (const e of earnings) {
        const vline = document.createElement("div");
        vline.className = "evt-vline";
        ov.appendChild(vline);

        const badge = document.createElement("div");
        const isScheduled = e.status === "scheduled";
        const miss = !isScheduled && (e.eps_surprise_pct ?? 0) < 0;
        badge.className = "evt-badge" + (isScheduled ? " scheduled" : (miss ? " miss" : ""));
        badge.textContent = "E";
        badge._evt = e;
        badge._vline = vline;
        ov.appendChild(badge);

        badge.addEventListener("mouseenter", () => showEventTooltip(e, badge, vline));
        badge.addEventListener("mouseleave", () => hideEventTooltip(vline));
    }
    positionEarningsOverlay();
}

/** Reposition every badge + vline according to current timeScale mapping. */
function positionEarningsOverlay() {
    const ov = document.querySelector(".evt-overlay");
    if (!ov || !STATE.chart) return;
    const ts = STATE.chart.timeScale();
    const badges = ov.querySelectorAll(".evt-badge");
    for (const b of badges) {
        const e = b._evt;
        const x = ts.timeToCoordinate(e.ts_date);
        if (x == null) {
            b.style.display = "none";
            b._vline.style.display = "none";
            continue;
        }
        b.style.display = "";
        b.style.left = `${x}px`;
        b._vline.style.display = "";
        b._vline.style.left = `${x}px`;
    }
}

function showEventTooltip(e, badge, vline) {
    const ov = document.querySelector(".evt-overlay");
    const tip = ov.querySelector(".evt-tooltip");
    vline.classList.add("shown");
    const isScheduled = e.status === "scheduled";
    // Surprise in dollar terms = actual EPS − estimate EPS
    const surpDollar = (!isScheduled && e.eps_actual != null && e.eps_estimate != null)
        ? (Number(e.eps_actual) - Number(e.eps_estimate))
        : null;
    const surpClass = surpDollar == null ? "" : (surpDollar >= 0 ? "up" : "down");
    const q = e.quarter ? `Q${e.quarter} ` : "";
    const title = `Earnings ${q}${e.ts_date}`;
    const rows = [`<div class="title">${title}</div>`];
    if (e.eps_estimate != null) rows.push(`<div><span class="k">Expected</span><span class="v">$${Number(e.eps_estimate).toFixed(2)}</span></div>`);
    if (!isScheduled && e.eps_actual != null) rows.push(`<div><span class="k">Actual</span><span class="v">$${Number(e.eps_actual).toFixed(2)}</span></div>`);
    if (surpDollar != null) rows.push(`<div><span class="k">Surprise</span><span class="v ${surpClass}">${surpDollar >= 0 ? "+" : "-"}$${Math.abs(surpDollar).toFixed(2)}</span></div>`);
    tip.innerHTML = rows.join("");

    // Position the tooltip near the badge; default to upper-right of the badge,
    // flip to left if it would overflow the chart area.
    const ovRect = ov.getBoundingClientRect();
    const bRect  = badge.getBoundingClientRect();
    const tipW   = 180;   // approximate; CSS min-width governs
    let left = bRect.left - ovRect.left + 14;
    if (left + tipW > ovRect.width) left = bRect.left - ovRect.left - tipW - 4;
    // Place ~middle vertically
    tip.style.left = `${left}px`;
    tip.style.bottom = `28px`;
    tip.style.top = "auto";
    tip.classList.add("shown");
}

function hideEventTooltip(vline) {
    const ov = document.querySelector(".evt-overlay");
    const tip = ov?.querySelector(".evt-tooltip");
    if (vline) vline.classList.remove("shown");
    if (tip)   tip.classList.remove("shown");
}

// ---------- toolbar --------------------------------------------------------
function wireToolbar() {
    $("tf-group").addEventListener("click", e => {
        const b = e.target.closest("button[data-tf]");
        if (!b) return;
        STATE.tf = b.dataset.tf;
        document.querySelectorAll("#tf-group button").forEach(x => x.classList.toggle("active", x === b));
        loadBars();
    });
    $("ind-group").addEventListener("click", e => {
        const b = e.target.closest("button[data-ind]");
        if (!b) return;
        const ind = b.dataset.ind;
        if (STATE.indicators.has(ind)) STATE.indicators.delete(ind);
        else STATE.indicators.add(ind);
        b.classList.toggle("active");
        if (SUBPANE_INDICATORS[ind]) updatePanes();
        loadBars();
    });
    $("tz-group").addEventListener("click", e => {
        const b = e.target.closest("button[data-tz]");
        if (!b) return;
        STATE.tz = b.dataset.tz;
        document.querySelectorAll("#tz-group button").forEach(x => x.classList.toggle("active", x === b));
        applyTimezoneToChart();
    });
    $("evt-group").addEventListener("click", async e => {
        const b = e.target.closest("button[data-evt]");
        if (!b) return;
        const k = b.dataset.evt;
        if (STATE.events.has(k)) STATE.events.delete(k);
        else STATE.events.add(k);
        b.classList.toggle("active");
        await ensureEventData();
        applyEventMarkers();
    });
    $("type-group").addEventListener("click", e => {
        const b = e.target.closest("button[data-type]");
        if (!b) return;
        document.querySelectorAll("#type-group button").forEach(x => x.classList.toggle("active", x === b));
        STATE.type = b.dataset.type;
        // swap series type
        if (STATE.priceSeries) STATE.chart.removeSeries(STATE.priceSeries);
        if (STATE.type === "candles") {
            STATE.priceSeries = STATE.chart.addCandlestickSeries({
                upColor: "#30d158", downColor: "#ff3b30",
                wickUpColor: "#30d158", wickDownColor: "#ff3b30",
                borderUpColor: "#30d158", borderDownColor: "#ff3b30",
            });
        } else {
            STATE.priceSeries = STATE.chart.addLineSeries({
                color: "#f0f0f0", lineWidth: 1,
            });
        }
        loadBars();
    });
}

// ---------- tabs -----------------------------------------------------------
const TAB_RENDERERS = {
    overview: renderOverview,
    stats: renderStats,
    events: renderEvents,
    macro: renderMacro,
    metadata: renderMetadata,
    integrity: renderIntegrity,
    raw: renderRaw,
};
let activeTab = "overview";

function wireTabs() {
    $("tabs").addEventListener("click", e => {
        const b = e.target.closest("button[data-tab]");
        if (!b) return;
        document.querySelectorAll("#tabs button").forEach(x => x.classList.toggle("active", x === b));
        activeTab = b.dataset.tab;
        loadActiveTab();
    });
}

async function loadActiveTab() {
    if (!STATE.symbol) return;
    const body = $("tab-body");
    body.innerHTML = "Loading…";
    try {
        await TAB_RENDERERS[activeTab](body);
    } catch (e) {
        body.innerHTML = `<div style="color:var(--down)">Error: ${e.message}</div>`;
    }
}

async function renderOverview(body) {
    // Parallel fetch — includes the new data types
    const [a, s, ca, er, sv] = await Promise.all([
        fetch(`/api/assets`).then(r => r.json()).then(rows => rows.find(x => x.symbol === STATE.symbol)),
        api(`/api/assets/${STATE.symbol}/stats`),
        api(`/api/assets/${STATE.symbol}/corporate_actions`).catch(() => null),
        api(`/api/assets/${STATE.symbol}/earnings`).catch(() => null),
        api(`/api/assets/${STATE.symbol}/short_volume`).catch(() => null),
    ]);
    const cls = signClass(a.change);

    // Derived tiles
    const divYield = (ca?.ttm_dividend && a.last_price)
        ? (ca.ttm_dividend / a.last_price * 100) : null;
    const lastErn = er?.summary?.last;
    const shortLatest = sv?.summary?.latest_short_pct;
    const shortMean60 = sv?.summary?.mean_short_pct_last_60d;

    body.innerHTML = `
        <div class="section-title">Quote</div>
        <div class="kv-grid">
            <div class="k">Symbol</div><div class="v">${a.symbol}</div>
            <div class="k">Bars</div><div class="v num">${fmtInt(a.bars)}</div>

            <div class="k">Last Price</div><div class="v num">${fmtPrice(a.last_price)}</div>
            <div class="k">Prev Close</div><div class="v num">${fmtPrice(a.prev_close)}</div>

            <div class="k">Change</div><div class="v num ${cls}">${fmtSigned(a.change)} ${fmtPct(a.change_pct)}</div>
            <div class="k">Last Volume</div><div class="v num">${fmtInt(a.last_volume)}</div>

            <div class="k">First Bar</div><div class="v num">${(a.first_bar || "").replace("T"," ").slice(0,19)}</div>
            <div class="k">Last Bar</div><div class="v num">${(a.last_bar || "").replace("T"," ").slice(0,19)}</div>

            <div class="k">Trading Days</div><div class="v num">${fmtInt(a.trading_days)}</div>
            <div class="k">Period Return</div><div class="v num ${signClass(s.price?.change_pct)}">${fmtPct(s.price?.change_pct)}</div>
        </div>

        <div class="section-title">Period Range (since ${(s.first_ts || "").slice(0,10)})</div>
        <div class="kv-grid">
            <div class="k">High</div><div class="v num">${fmtPrice(s.price?.period_high)}</div>
            <div class="k">Low</div><div class="v num">${fmtPrice(s.price?.period_low)}</div>
            <div class="k">Ann. Vol</div><div class="v num">${fmtPct(s.returns?.annualized_vol_pct)}</div>
            <div class="k">Max DD</div><div class="v num down">${fmtPct(s.returns?.max_drawdown_pct)}</div>
        </div>

        <div class="section-title">Shareholder Return</div>
        <div class="kv-grid">
            <div class="k">TTM Dividend</div><div class="v num">${ca?.ttm_dividend != null ? "$" + ca.ttm_dividend.toFixed(2) : "—"}</div>
            <div class="k">Div Yield (TTM)</div><div class="v num">${divYield != null ? divYield.toFixed(2) + "%" : "—"}</div>
            <div class="k"># Dividends</div><div class="v num">${ca?.n_dividends ?? "—"}</div>
            <div class="k"># Splits</div><div class="v num">${ca?.n_splits ?? "—"}</div>
        </div>

        <div class="section-title">Earnings</div>
        <div class="kv-grid">
            <div class="k">Last Report</div><div class="v num">${lastErn ? lastErn.earnings_ts?.slice(0,16) : "—"}</div>
            <div class="k">Last EPS Actual</div><div class="v num">${lastErn?.eps_actual != null ? "$" + lastErn.eps_actual.toFixed(2) : "—"}</div>
            <div class="k">Last EPS Est</div><div class="v num">${lastErn?.eps_estimate != null ? "$" + lastErn.eps_estimate.toFixed(2) : "—"}</div>
            <div class="k">Last Surprise</div><div class="v num ${signClass(lastErn?.eps_surprise_pct)}">${lastErn?.eps_surprise_pct != null ? fmtPct(lastErn.eps_surprise_pct) : "—"}</div>
            <div class="k">Beat Rate</div><div class="v num">${er?.summary?.beat_rate_pct != null ? er.summary.beat_rate_pct + "%" : "—"}</div>
            <div class="k">Reports Stored</div><div class="v num">${er?.n ?? "—"}</div>
        </div>

        <div class="section-title">Short Interest (FINRA daily regulatory short volume)</div>
        <div class="kv-grid">
            <div class="k">Latest Short %</div><div class="v num ${shortLatest > 50 ? "down" : ""}">${shortLatest != null ? shortLatest.toFixed(2) + "%" : "—"}</div>
            <div class="k">60d Mean</div><div class="v num">${shortMean60 != null ? shortMean60.toFixed(2) + "%" : "—"}</div>
            <div class="k">Full-history Mean</div><div class="v num">${sv?.summary?.mean_short_pct_full_history != null ? sv.summary.mean_short_pct_full_history.toFixed(2) + "%" : "—"}</div>
            <div class="k">Days Covered</div><div class="v num">${fmtInt(sv?.summary?.n)}</div>
        </div>
    `;
}

async function renderStats(body) {
    const s = await api(`/api/assets/${STATE.symbol}/stats`);
    if (s.empty) { body.textContent = "No data."; return; }
    body.innerHTML = `
        <div class="section-title">Price</div>
        <div class="kv-grid">
            <div class="k">First</div><div class="v num">${fmtPrice(s.price.first)}</div>
            <div class="k">Last</div><div class="v num">${fmtPrice(s.price.last)}</div>
            <div class="k">Period High</div><div class="v num">${fmtPrice(s.price.period_high)}</div>
            <div class="k">Period Low</div><div class="v num">${fmtPrice(s.price.period_low)}</div>
            <div class="k">Total Change</div><div class="v num ${signClass(s.price.change)}">${fmtSigned(s.price.change)}</div>
            <div class="k">Total Return</div><div class="v num ${signClass(s.price.change_pct)}">${fmtPct(s.price.change_pct)}</div>
        </div>

        <div class="section-title">Returns (daily)</div>
        <div class="kv-grid">
            <div class="k">N Days</div><div class="v num">${fmtInt(s.returns.n_days)}</div>
            <div class="k">Mean Daily</div><div class="v num">${fmtPct(s.returns.mean_daily_pct, 4)}</div>
            <div class="k">Std Daily</div><div class="v num">${fmtPct(s.returns.std_daily_pct, 4)}</div>
            <div class="k">Ann. Vol</div><div class="v num">${fmtPct(s.returns.annualized_vol_pct)}</div>
            <div class="k">Ann. Return</div><div class="v num ${signClass(s.returns.annualized_return_pct)}">${fmtPct(s.returns.annualized_return_pct)}</div>
            <div class="k">Sharpe ≈</div><div class="v num">${s.returns.sharpe_approx == null ? "—" : s.returns.sharpe_approx.toFixed(3)}</div>
            <div class="k">Max Drawdown</div><div class="v num down">${fmtPct(s.returns.max_drawdown_pct)}</div>
            <div class="k">Up / Down</div><div class="v num">${fmtInt(s.returns.up_days)} / ${fmtInt(s.returns.down_days)}</div>
        </div>

        <div class="section-title">Volume</div>
        <div class="kv-grid">
            <div class="k">Total</div><div class="v num">${fmtInt(s.volume.total)}</div>
            <div class="k">Mean Daily</div><div class="v num">${fmtInt(Math.round(s.volume.mean_daily))}</div>
            <div class="k">Median Daily</div><div class="v num">${fmtInt(Math.round(s.volume.median_daily))}</div>
            <div class="k">Source</div><div class="v">${(s.sources || []).join(", ") || "—"}</div>
        </div>
    `;
}

async function renderEvents(body) {
    const [ca, er, sv] = await Promise.all([
        api(`/api/assets/${STATE.symbol}/corporate_actions`).catch(() => ({dividends:[], splits:[]})),
        api(`/api/assets/${STATE.symbol}/earnings`).catch(() => ({rows:[]})),
        api(`/api/assets/${STATE.symbol}/short_volume?limit=30`).catch(() => ({rows:[]})),
    ]);

    const divRows = (ca.dividends || []).slice().reverse().slice(0, 40).map(d => `
        <tr><td class="num">${d.ts_date}</td><td class="num">$${Number(d.dividend_amount).toFixed(2)}</td></tr>`).join("");
    const splitRows = (ca.splits || []).slice().reverse().map(s => `
        <tr><td class="num">${s.ts_date}</td><td class="num">${s.split_ratio}:1</td></tr>`).join("");
    const ernRows = (er.rows || []).slice(0, 40).map(e => `
        <tr>
            <td class="num">${(e.earnings_ts||"").slice(0,16)}</td>
            <td class="num">${e.eps_actual != null ? "$" + Number(e.eps_actual).toFixed(2) : "—"}</td>
            <td class="num">${e.eps_estimate != null ? "$" + Number(e.eps_estimate).toFixed(2) : "—"}</td>
            <td class="num ${signClass(e.eps_surprise_pct)}">${e.eps_surprise_pct != null ? fmtPct(Number(e.eps_surprise_pct)) : "—"}</td>
        </tr>`).join("");
    const svRows = (sv.rows || []).slice().reverse().slice(0, 15).map(r => `
        <tr>
            <td class="num">${r.ts_date}</td>
            <td class="num">${fmtInt(Math.round(r.short_volume))}</td>
            <td class="num">${fmtInt(Math.round(r.total_volume))}</td>
            <td class="num ${Number(r.short_pct) > 50 ? "down" : ""}">${Number(r.short_pct).toFixed(2)}%</td>
        </tr>`).join("");

    body.innerHTML = `
        <div class="section-title">Earnings — ${er.n || 0} reports (${er.summary?.beats || 0} beats / ${er.summary?.misses || 0} misses · beat rate ${er.summary?.beat_rate_pct ?? "—"}%)</div>
        <table class="tab-table">
          <thead><tr><th>Reported (ET)</th><th>EPS Actual</th><th>EPS Estimate</th><th>Surprise</th></tr></thead>
          <tbody>${ernRows || `<tr><td colspan="4">No data.</td></tr>`}</tbody>
        </table>

        <div class="section-title">Dividends — ${ca.n_dividends || 0} events (TTM $${(ca.ttm_dividend ?? 0).toFixed(2)})</div>
        <table class="tab-table">
          <thead><tr><th>Ex-Date</th><th>Amount</th></tr></thead>
          <tbody>${divRows || `<tr><td colspan="2">No data.</td></tr>`}</tbody>
        </table>

        <div class="section-title">Splits — ${ca.n_splits || 0} events</div>
        <table class="tab-table">
          <thead><tr><th>Date</th><th>Ratio</th></tr></thead>
          <tbody>${splitRows || `<tr><td colspan="2">No data.</td></tr>`}</tbody>
        </table>

        <div class="section-title">Short Volume — last 15 sessions</div>
        <table class="tab-table">
          <thead><tr><th>Date</th><th>Short Vol</th><th>Total Vol</th><th>Short %</th></tr></thead>
          <tbody>${svRows || `<tr><td colspan="4">No data.</td></tr>`}</tbody>
        </table>
    `;
}

async function renderMacro(body) {
    const [macroList, assetsList] = await Promise.all([
        api("/api/macro/series"),
        api("/api/overview"),
    ]);
    const benchmarkSyms = ["SPY","QQQ","IWM","TLT","GLD","UUP","VIX"]
        .filter(s => assetsList.symbols?.includes(s) || true);  // show all; backend 404s gracefully

    const macroRows = macroList.map(m => `
        <tr>
          <td class="num">${m.series_id}</td>
          <td>${m.description || ""}</td>
          <td>${m.frequency}</td>
          <td class="num">${m.latest_value != null ? m.latest_value.toFixed(3) : "—"}</td>
          <td class="num">${m.last_date || "—"}</td>
          <td class="num">${fmtInt(m.n)}</td>
          <td class="num">${m.period_low != null ? m.period_low.toFixed(2) : "—"} / ${m.period_high != null ? m.period_high.toFixed(2) : "—"}</td>
        </tr>`).join("");

    // Pull latest close for each benchmark
    const benchCells = await Promise.all(benchmarkSyms.map(async s => {
        try {
            const d = await api(`/api/index/${s}/bars_1d`);
            if (!d.rows?.length) return { sym: s, last: null };
            const last = d.rows[d.rows.length - 1];
            const prev = d.rows[d.rows.length - 2];
            const chg = prev ? (last.close - prev.close) : null;
            const chgp = prev ? (chg / prev.close * 100) : null;
            return { sym: s, last: last.close, date: last.ts_date, chg, chgp };
        } catch { return { sym: s, last: null }; }
    }));
    const benchRows = benchCells.map(b => `
        <tr>
          <td class="num">${b.sym}</td>
          <td class="num">${b.last != null ? fmtPrice(b.last) : "—"}</td>
          <td class="num">${b.date || "—"}</td>
          <td class="num ${signClass(b.chg)}">${b.chg != null ? fmtSigned(b.chg) : "—"}</td>
          <td class="num ${signClass(b.chgp)}">${b.chgp != null ? fmtPct(b.chgp) : "—"}</td>
        </tr>`).join("");

    body.innerHTML = `
        <div class="section-title">Index / ETF Benchmarks (daily)</div>
        <table class="tab-table">
          <thead><tr><th>Symbol</th><th>Last</th><th>Date</th><th>Δ</th><th>Δ%</th></tr></thead>
          <tbody>${benchRows || `<tr><td colspan="5">No benchmark data.</td></tr>`}</tbody>
        </table>

        <div class="section-title">Macro Series (FRED)</div>
        <table class="tab-table">
          <thead><tr><th>Series</th><th>Description</th><th>Freq</th><th>Latest</th><th>Last Date</th><th>N</th><th>Min / Max</th></tr></thead>
          <tbody>${macroRows || `<tr><td colspan="7">No macro data.</td></tr>`}</tbody>
        </table>
    `;
}

async function renderMetadata(body) {
    body.innerHTML = "Fetching from yfinance (cached after first load)…";
    const m = await api(`/api/assets/${STATE.symbol}/metadata`);
    if (m._error) { body.innerHTML = `<div style="color:var(--down)">${m._error}</div>`; return; }
    body.innerHTML = `
        <div class="section-title">Identity</div>
        <div class="kv-grid">
            <div class="k">Symbol</div><div class="v">${m.symbol}</div>
            <div class="k">Name</div><div class="v">${m.longName || m.shortName || "—"}</div>
            <div class="k">Type</div><div class="v">${m.quoteType || "—"}</div>
            <div class="k">Exchange</div><div class="v">${m.exchange || "—"}</div>
            <div class="k">Currency</div><div class="v">${m.currency || "—"}</div>
            <div class="k">Country</div><div class="v">${m.country || "—"}</div>
            <div class="k">Sector</div><div class="v">${m.sector || "—"}</div>
            <div class="k">Industry</div><div class="v">${m.industry || "—"}</div>
        </div>

        <div class="section-title">Key Numbers</div>
        <div class="kv-grid">
            <div class="k">Market Cap</div><div class="v num">${fmtLargeNum(m.marketCap)}</div>
            <div class="k">Shares Out</div><div class="v num">${fmtLargeNum(m.sharesOutstanding)}</div>
            <div class="k">Trailing P/E</div><div class="v num">${m.trailingPE?.toFixed(2) || "—"}</div>
            <div class="k">Forward P/E</div><div class="v num">${m.forwardPE?.toFixed(2) || "—"}</div>
            <div class="k">Beta</div><div class="v num">${m.beta?.toFixed(2) || "—"}</div>
            <div class="k">Dividend Yield</div><div class="v num">${m.dividendYield != null ? (m.dividendYield*100).toFixed(2)+"%" : "—"}</div>
            <div class="k">52W High</div><div class="v num">${fmtPrice(m["52WeekHigh"])}</div>
            <div class="k">52W Low</div><div class="v num">${fmtPrice(m["52WeekLow"])}</div>
            <div class="k">Avg Volume</div><div class="v num">${fmtLargeNum(m.averageVolume)}</div>
            <div class="k">Website</div><div class="v">${m.website ? `<a href="${m.website}" target="_blank">${m.website}</a>` : "—"}</div>
        </div>

        ${m.longBusinessSummary ? `
        <div class="section-title">Business</div>
        <div class="longtext">${m.longBusinessSummary}</div>` : ""}

        <div class="section-title" style="margin-top:14px">Cache</div>
        <div class="kv-grid">
            <div class="k">Fetched</div><div class="v num">${m.fetched_at_utc || "—"}</div>
        </div>
    `;
}

async function renderIntegrity(body) {
    const i = await api(`/api/assets/${STATE.symbol}/integrity`);
    if (!i.manifest_exists) { body.textContent = "No manifest."; return; }
    const vr = i.last_verification_report;

    const rows = i.entries.map(e => `
        <tr>
            <td class="num">${e.path.split("/").slice(-3, -1).join("/")}</td>
            <td class="num">${fmtInt(e.rows)}</td>
            <td class="num">${e.min_ts_utc?.replace("T"," ").slice(0,19)}</td>
            <td class="num">${e.max_ts_utc?.replace("T"," ").slice(0,19)}</td>
            <td class="num">${e._mode}</td>
            <td class="num">${fmtLargeNum(e._size)}</td>
            <td class="sha">${e.sha256.slice(0,16)}…</td>
            <td><span class="badge ${e._status}">${e._status}</span></td>
        </tr>
    `).join("");

    body.innerHTML = `
        <div class="section-title">Manifest Summary</div>
        <div class="kv-grid">
            <div class="k">Entries</div><div class="v num">${i.n_entries}</div>
            <div class="k">OK</div><div class="v num up">${i.n_ok}</div>
            <div class="k">Tampered</div><div class="v num ${i.n_tampered ? "down" : ""}">${i.n_tampered}</div>
            <div class="k">Missing</div><div class="v num ${i.n_missing ? "down" : ""}">${i.n_missing}</div>
        </div>

        ${vr ? `
        <div class="section-title">Last Verification Report (${vr.run_at_utc?.slice(0,19)} UTC)</div>
        <div class="kv-grid">
            <div class="k">Total Bars</div><div class="v num">${fmtInt(vr.internal_consistency?.total_bars)}</div>
            <div class="k">Duplicate TS</div><div class="v num">${vr.internal_consistency?.duplicate_timestamps}</div>
            <div class="k">Days Expected</div><div class="v num">${vr.completeness?.expected_trading_days}</div>
            <div class="k">Days Present</div><div class="v num">${vr.completeness?.present_trading_days}</div>
            <div class="k">Days Missing</div><div class="v num ${vr.completeness?.missing_days_count ? "down" : ""}">${vr.completeness?.missing_days_count}</div>
            <div class="k">Close vs yf median</div><div class="v num">$${vr.price_accuracy?.close_diff_median}</div>
            <div class="k">Close vs yf p95</div><div class="v num">$${vr.price_accuracy?.close_diff_abs_p95}</div>
            <div class="k">Vol Ratio vs yf (median)</div><div class="v num">${vr.price_accuracy?.volume_ratio_median_pct}%</div>
        </div>` : ""}

        <div class="section-title">Files</div>
        <table class="tab-table">
            <thead><tr>
                <th>Path</th><th>Rows</th><th>Min TS</th><th>Max TS</th><th>Mode</th><th>Bytes</th><th>SHA256</th><th>Status</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `;
}

async function renderRaw(body) {
    const { page, page_size, order } = STATE.raw;
    const r = await api(`/api/assets/${STATE.symbol}/bars/raw?page=${page}&page_size=${page_size}&order=${order}`);
    const rows = r.rows.map(x => `
        <tr>
            <td class="num">${x.ts_utc}</td>
            <td class="num">${fmtPrice(x.open)}</td>
            <td class="num">${fmtPrice(x.high)}</td>
            <td class="num">${fmtPrice(x.low)}</td>
            <td class="num">${fmtPrice(x.close)}</td>
            <td class="num">${fmtInt(x.volume)}</td>
            <td class="num">${fmtInt(x.trade_count)}</td>
            <td class="num">${fmtPrice(x.vwap, 4)}</td>
            <td>${x.source}</td>
        </tr>`).join("");

    const maxPage = Math.max(0, Math.ceil(r.total / r.page_size) - 1);

    body.innerHTML = `
        <div class="section-title">Raw 1-min Bars · ${fmtInt(r.total)} total · page ${r.page + 1} / ${maxPage + 1}</div>
        <table class="tab-table">
            <thead><tr>
                <th>TS UTC</th><th>Open</th><th>High</th><th>Low</th><th>Close</th>
                <th>Vol</th><th>Trades</th><th>VWAP</th><th>Src</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
        <div class="pager">
            <button id="raw-first" ${page === 0 ? "disabled" : ""}>⏮</button>
            <button id="raw-prev"  ${page === 0 ? "disabled" : ""}>◀</button>
            <span>Page <span class="num">${page + 1}</span> of <span class="num">${maxPage + 1}</span></span>
            <button id="raw-next"  ${page >= maxPage ? "disabled" : ""}>▶</button>
            <button id="raw-last"  ${page >= maxPage ? "disabled" : ""}>⏭</button>
            <select id="raw-order">
                <option value="desc" ${order==="desc"?"selected":""}>Newest first</option>
                <option value="asc"  ${order==="asc"?"selected":""}>Oldest first</option>
            </select>
        </div>
    `;
    $("raw-first").onclick = () => { STATE.raw.page = 0; loadActiveTab(); };
    $("raw-prev").onclick  = () => { STATE.raw.page = Math.max(0, page - 1); loadActiveTab(); };
    $("raw-next").onclick  = () => { STATE.raw.page = Math.min(maxPage, page + 1); loadActiveTab(); };
    $("raw-last").onclick  = () => { STATE.raw.page = maxPage; loadActiveTab(); };
    $("raw-order").onchange = (e) => { STATE.raw.order = e.target.value; STATE.raw.page = 0; loadActiveTab(); };
}

// ---------- keyboard shortcuts --------------------------------------------
document.addEventListener("keydown", e => {
    if (e.target.tagName === "INPUT" || e.target.tagName === "SELECT") return;
    const tfMap = { "1":"1m","2":"5m","3":"15m","4":"30m","5":"1h","6":"4h","7":"1D","8":"1W" };
    if (tfMap[e.key]) {
        STATE.tf = tfMap[e.key];
        document.querySelectorAll("#tf-group button").forEach(x => x.classList.toggle("active", x.dataset.tf === STATE.tf));
        loadBars();
    }
});

// ---------- init -----------------------------------------------------------
createChart();
wireToolbar();
wireTabs();
// set tf default-active class already set in html
// default indicators — none
loadAssets();

// DEBUG hook — remove in production
window.__DEBUG = { STATE };

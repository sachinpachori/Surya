const chartCountSelect = document.getElementById("chartCount");
const chartGrid = document.getElementById("chartGrid");
const marketStrip = document.getElementById("marketStrip");
const storageKey = "tradingDashboardChartCount";

let config = null;
let eventSource = null;
let paneStates = [];

Object.defineProperty(window, "paneStates", { get: () => paneStates });

const layoutRules = {
    1: { columns: 1 },
    2: { columns: 2 },
    4: { columns: 2 },
    6: { columns: 3 },
    8: { columns: 4 },
};

const indicatorGroups = [
    {
        title: "Moving Averages",
        items: [
            { id: "ema9", label: "EMA (9)", color: "#22c55e", type: "ema", period: 9 },
            { id: "ema50", label: "EMA (50)", color: "#8b5cf6", type: "ema", period: 50 },
            { id: "ema200", label: "EMA (200)", color: "#f472b6", type: "ema", period: 200 },
            { id: "vwap", label: "VWAP", color: "#fde047", type: "vwap" },
        ],
    },
    {
        title: "Bands & Channels",
        items: [
            { id: "bb20", label: "Bollinger (20, 2)", color: "#94a3b8", type: "bollinger", period: 20, deviation: 2 },
            { id: "donchian20", label: "Donchian (20)", color: "#2dd4bf", type: "donchian", period: 20 },
            { id: "keltner20", label: "Keltner (20, 1.5)", color: "#f9a8d4", type: "keltner", period: 20, multiplier: 1.5 },
        ],
    },
    {
        title: "Trend",
        items: [
            { id: "sar", label: "Parabolic SAR", color: "#fbbf24", type: "sar" },
            { id: "supertrend", label: "Supertrend (10, 3)", color: "#a78bfa", type: "supertrend", period: 10, multiplier: 3 },
            { id: "ichimoku", label: "Ichimoku Cloud", color: "#22d3ee", type: "ichimoku" },
            { id: "pivots", label: "Pivot Points", color: "#cbd5e1", type: "pivots" },
        ],
    },
    {
        title: "Price Action",
        items: [
            { id: "fvg", label: "Fair Value Gaps", color: "#fde047", type: "fvg" },
            { id: "volumeProfile", label: "Volume Profile (POC/VA)", color: "#a3e635", type: "volumeProfile" },
        ],
    },
    {
        title: "Volume",
        items: [
            { id: "volume", label: "Volume", color: "#64748b", type: "volume", enabled: true },
        ],
    },
];

function getStoredCount() {
    const stored = localStorage.getItem(storageKey);
    return stored && ["1", "2", "4", "6", "8"].includes(stored) ? Number(stored) : 4;
}

async function loadConfig() {
    const res = await fetch("/api/config");
    config = await res.json();
}

function sourceLabel(id) {
    return (config.sources.find((source) => source.id === id) || {}).label || id;
}

function normalizeSymbol(source, symbol) {
    if (!symbol) return "";
    if (source === "binance") return symbol.toUpperCase().replace(/\//g, "");
    return symbol.toUpperCase().replace(/\s+/g, "");
}

function formatPrice(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    if (num >= 1000) return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (num >= 1) return num.toFixed(2);
    return num.toFixed(6);
}

function timeframeSeconds(timeframe) {
    return { "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400 }[timeframe] || 300;
}

function candleBucket(timestamp, timeframe) {
    const seconds = timeframeSeconds(timeframe);
    return Math.floor(timestamp / seconds) * seconds;
}

function lineData(values) {
    return values.filter((point) => Number.isFinite(point.value));
}

function emaData(bars, period) {
    const k = 2 / (period + 1);
    let ema = null;
    return lineData(bars.map((bar, index) => {
        ema = ema === null ? bar.close : bar.close * k + ema * (1 - k);
        return { time: bar.time, value: index + 1 >= period ? ema : NaN };
    }));
}

function smaAt(bars, index, period, key = "close") {
    if (index + 1 < period) return NaN;
    let total = 0;
    for (let i = index - period + 1; i <= index; i += 1) total += bars[i][key];
    return total / period;
}

function atrData(bars, period) {
    const trs = bars.map((bar, index) => {
        if (index === 0) return bar.high - bar.low;
        return Math.max(bar.high - bar.low, Math.abs(bar.high - bars[index - 1].close), Math.abs(bar.low - bars[index - 1].close));
    });
    return bars.map((_, index) => {
        if (index + 1 < period) return NaN;
        let total = 0;
        for (let i = index - period + 1; i <= index; i += 1) total += trs[i];
        return total / period;
    });
}

function dayBucket(time) {
    return Math.floor(time / 86400);
}

function classicPivotSets(bars) {
    if (bars.length < 2) return [];
    const lastBucket = dayBucket(bars[bars.length - 1].time);
    const previousDayBars = bars.filter((bar) => dayBucket(bar.time) < lastBucket);
    let pivotBars = [];
    if (previousDayBars.length) {
        const previousBucket = dayBucket(previousDayBars[previousDayBars.length - 1].time);
        pivotBars = previousDayBars.filter((bar) => dayBucket(bar.time) === previousBucket);
    } else {
        pivotBars = bars.slice(Math.max(0, bars.length - 30), Math.max(1, bars.length - 1));
    }
    if (!pivotBars.length) return [];

    const high = Math.max(...pivotBars.map((bar) => bar.high));
    const low = Math.min(...pivotBars.map((bar) => bar.low));
    const close = pivotBars[pivotBars.length - 1].close;
    const pivot = (high + low + close) / 3;
    const range = high - low;
    return [
        { label: "R3", value: high + 2 * (pivot - low), color: "#f87171" },
        { label: "R2", value: pivot + range, color: "#fb7185" },
        { label: "R1", value: 2 * pivot - low, color: "#fda4af" },
        { label: "P", value: pivot, color: "#e2e8f0", width: 3 },
        { label: "S1", value: 2 * pivot - high, color: "#86efac" },
        { label: "S2", value: pivot - range, color: "#4ade80" },
        { label: "S3", value: low - 2 * (high - pivot), color: "#22c55e" },
    ].filter((level) => Number.isFinite(level.value));
}

function fvgZones(bars) {
    const zones = [];
    const lastTime = bars[bars.length - 1].time;
    for (let index = 2; index < bars.length; index += 1) {
        const first = bars[index - 2];
        const third = bars[index];
        let zone = null;
        if (first.high < third.low) {
            zone = {
                type: "Bull FVG",
                start: bars[index - 1].time,
                end: lastTime,
                top: third.low,
                bottom: first.high,
                color: "#facc15",
            };
        } else if (first.low > third.high) {
            zone = {
                type: "Bear FVG",
                start: bars[index - 1].time,
                end: lastTime,
                top: first.low,
                bottom: third.high,
                color: "#fb7185",
            };
        }
        if (!zone) continue;
        for (let futureIndex = index + 1; futureIndex < bars.length; futureIndex += 1) {
            const future = bars[futureIndex];
            const filled = zone.type === "Bull FVG" ? future.low <= zone.bottom : future.high >= zone.top;
            if (filled) {
                zone.end = future.time;
                break;
            }
        }
        if (zone.end > zone.start) zones.push(zone);
    }
    return zones.slice(-10);
}

function indicatorSeriesData(bars, spec) {
    if (!bars.length) return [];
    if (spec.type === "ema") return [emaData(bars, spec.period)];
    if (spec.type === "vwap") {
        let pv = 0;
        let volume = 0;
        return [lineData(bars.map((bar) => {
            const typical = (bar.high + bar.low + bar.close) / 3;
            const v = bar.volume || 0;
            pv += typical * v;
            volume += v;
            return { time: bar.time, value: volume ? pv / volume : typical };
        }))];
    }
    if (spec.type === "bollinger") {
        const middle = [];
        const upper = [];
        const lower = [];
        bars.forEach((bar, index) => {
            const avg = smaAt(bars, index, spec.period);
            if (!Number.isFinite(avg)) return;
            let variance = 0;
            for (let i = index - spec.period + 1; i <= index; i += 1) variance += Math.pow(bars[i].close - avg, 2);
            const band = Math.sqrt(variance / spec.period) * spec.deviation;
            middle.push({ time: bar.time, value: avg });
            upper.push({ time: bar.time, value: avg + band });
            lower.push({ time: bar.time, value: avg - band });
        });
        return [upper, middle, lower];
    }
    if (spec.type === "donchian") {
        const high = [];
        const low = [];
        bars.forEach((bar, index) => {
            if (index + 1 < spec.period) return;
            const slice = bars.slice(index - spec.period + 1, index + 1);
            high.push({ time: bar.time, value: Math.max(...slice.map((item) => item.high)) });
            low.push({ time: bar.time, value: Math.min(...slice.map((item) => item.low)) });
        });
        return [high, low];
    }
    if (spec.type === "keltner") {
        const atr = atrData(bars, spec.period);
        const mid = lineData(bars.map((bar, index) => ({ time: bar.time, value: smaAt(bars, index, spec.period) })));
        const upper = lineData(bars.map((bar, index) => ({ time: bar.time, value: smaAt(bars, index, spec.period) + atr[index] * spec.multiplier })));
        const lower = lineData(bars.map((bar, index) => ({ time: bar.time, value: smaAt(bars, index, spec.period) - atr[index] * spec.multiplier })));
        return [upper, mid, lower];
    }
    if (spec.type === "supertrend") {
        const atr = atrData(bars, spec.period);
        return [lineData(bars.map((bar, index) => {
            const middle = (bar.high + bar.low) / 2;
            const direction = index && bar.close < bars[index - 1].close ? -1 : 1;
            return { time: bar.time, value: middle - direction * atr[index] * spec.multiplier };
        }))];
    }
    if (spec.type === "pivots") {
        const start = bars[0].time;
        const end = bars[bars.length - 1].time;
        return classicPivotSets(bars).map((level) => ({
            data: [{ time: start, value: level.value }, { time: end, value: level.value }],
            color: level.color,
            lineWidth: level.width || 2,
            lineStyle: level.label === "P" ? 0 : 1,
            lastValueVisible: true,
            title: level.label,
        }));
    }
    if (spec.type === "fvg") {
        return fvgZones(bars).flatMap((zone) => {
            const midpoint = (zone.top + zone.bottom) / 2;
            return [
                {
                    data: [{ time: zone.start, value: zone.top }, { time: zone.end, value: zone.top }],
                    color: zone.color,
                    lineWidth: 3,
                    lineStyle: 0,
                    title: zone.type,
                },
                {
                    data: [{ time: zone.start, value: zone.bottom }, { time: zone.end, value: zone.bottom }],
                    color: zone.color,
                    lineWidth: 3,
                    lineStyle: 0,
                    title: zone.type,
                },
                {
                    data: [{ time: zone.start, value: midpoint }, { time: zone.end, value: midpoint }],
                    color: zone.color,
                    lineWidth: 1,
                    lineStyle: 2,
                    title: "",
                },
            ];
        });
    }
    if (spec.type === "volumeProfile") {
        const lows = bars.map((bar) => bar.low);
        const highs = bars.map((bar) => bar.high);
        const min = Math.min(...lows);
        const max = Math.max(...highs);
        if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) return [];
        const bucketCount = 48;
        const bucketSize = (max - min) / bucketCount;
        const buckets = Array.from({ length: bucketCount }, (_, index) => ({
            price: min + bucketSize * (index + 0.5),
            volume: 0,
        }));
        bars.forEach((bar) => {
            const price = (bar.high + bar.low + bar.close) / 3;
            const index = Math.max(0, Math.min(bucketCount - 1, Math.floor((price - min) / bucketSize)));
            buckets[index].volume += bar.volume || 0;
        });
        const totalVolume = buckets.reduce((total, bucket) => total + bucket.volume, 0);
        if (!totalVolume) return [];
        const sorted = [...buckets].sort((a, b) => b.volume - a.volume);
        const poc = sorted[0].price;
        let valueAreaVolume = 0;
        const valueBuckets = [];
        for (const bucket of sorted) {
            valueBuckets.push(bucket.price);
            valueAreaVolume += bucket.volume;
            if (valueAreaVolume >= totalVolume * 0.7) break;
        }
        const vah = Math.max(...valueBuckets);
        const val = Math.min(...valueBuckets);
        const start = bars[0].time;
        const end = bars[bars.length - 1].time;
        return [
            [{ time: start, value: poc }, { time: end, value: poc }],
            [{ time: start, value: vah }, { time: end, value: vah }],
            [{ time: start, value: val }, { time: end, value: val }],
        ];
    }
    return [];
}

function setMarketStrip(statuses) {
    marketStrip.innerHTML = "";
    statuses.forEach((status) => {
        const chip = document.createElement("div");
        chip.className = `market-chip ${status.open ? "open" : "closed"}`;
        chip.innerHTML = `<span></span>${status.label} ${status.open ? "open" : "closed"}`;
        marketStrip.appendChild(chip);
    });
}

async function refreshMarketStrip() {
    const checks = [
        { label: "NSE", source: "yfinance_india", symbol: "RELIANCE.NS" },
        { label: "US market", source: "yfinance_us", symbol: "AAPL" },
        { label: "Crypto", source: "hyperliquid", symbol: "BTC-USDC" },
    ];
    const statuses = await Promise.all(checks.map(async (item) => {
        try {
            const res = await fetch(`/api/market_status?source=${encodeURIComponent(item.source)}&symbol=${encodeURIComponent(item.symbol)}`);
            return { ...item, ...(await res.json()) };
        } catch {
            return { ...item, open: false };
        }
    }));
    setMarketStrip(statuses);
}

function buildIndicatorDrawer(state) {
    const drawer = document.createElement("aside");
    drawer.className = "indicator-drawer";
    const list = indicatorGroups.map((group) => {
        const rows = group.items.map((item) => `
            <label class="indicator-row" data-indicator="${item.id}">
                <input type="checkbox" ${item.enabled ? "checked" : ""} />
                <span class="swatch" style="background:${item.color}"></span>
                <span>${item.label}</span>
            </label>
        `).join("");
        return `<div class="indicator-group"><div class="indicator-title">${group.title}</div>${rows}</div>`;
    }).join("");
    drawer.innerHTML = `<div class="drawer-scroll">${list}</div>`;
    drawer.querySelectorAll(".indicator-row").forEach((row) => {
        const input = row.querySelector("input");
        const id = row.dataset.indicator;
        state.indicators[id] = input.checked;
        input.addEventListener("change", () => {
            state.indicators[id] = input.checked;
            row.classList.toggle("active", input.checked);
            applyIndicators(state);
        });
        row.classList.toggle("active", input.checked);
    });
    return drawer;
}

function buildPanel(index) {
    const panel = document.createElement("section");
    panel.className = "chart-panel";
    panel.dataset.index = index;

    panel.innerHTML = `
        <div class="panel-header">
            <div class="panel-controls">
                <select id="source-${index}" aria-label="Data source"></select>
                <select id="symbolSelect-${index}" aria-label="Symbol"></select>
                <select id="timeframe-${index}" aria-label="Timeframe"></select>
                <button class="indicator-btn" type="button">INDICATORS</button>
            </div>
            <div class="price-pill">
                <span id="symbol-${index}">-</span>
                <strong id="price-${index}">-</strong>
                <em id="change-${index}">+0.00 (+0.00%)</em>
            </div>
        </div>
        <div class="chart-area">
            <div id="chart-${index}" class="chart-holder"></div>
        </div>
    `;
    chartGrid.appendChild(panel);

    const sourceSelect = panel.querySelector(`#source-${index}`);
    const symbolSelect = panel.querySelector(`#symbolSelect-${index}`);
    const timeSelect = panel.querySelector(`#timeframe-${index}`);
    const indicatorBtn = panel.querySelector(".indicator-btn");

    config.sources.forEach((source) => sourceSelect.add(new Option(source.label, source.id)));
    config.timeframes.forEach((interval) => timeSelect.add(new Option(interval, interval)));

    const defaultSourceId = index === 1 ? "yfinance_us" : index % 2 === 0 ? "yfinance_india" : "hyperliquid";
    const defaultSource = config.sources.find((item) => item.id === defaultSourceId) || config.sources[0];
    const state = {
        index,
        panel,
        chart: null,
        series: null,
        volumeSeries: null,
        indicatorSeries: [],
        source: defaultSource.id,
        symbol: (config.symbols[defaultSource.id] || [])[0],
        timeframe: index === 3 ? "1m" : "5m",
        bars: [],
        indicators: {},
        sourceSelect,
        symbolSelect,
        timeSelect,
        priceElement: panel.querySelector(`#price-${index}`),
        changeElement: panel.querySelector(`#change-${index}`),
        symbolElement: panel.querySelector(`#symbol-${index}`),
        chartContainer: panel.querySelector(`#chart-${index}`),
        livePoll: null,
    };
    paneStates.push(state);

    panel.querySelector(".chart-area").prepend(buildIndicatorDrawer(state));

    function populateSymbols() {
        symbolSelect.innerHTML = "";
        (config.symbols[state.source] || []).forEach((symbol) => symbolSelect.add(new Option(symbol, symbol)));
        state.symbol = symbolSelect.value;
    }

    sourceSelect.value = state.source;
    populateSymbols();
    timeSelect.value = state.timeframe;

    sourceSelect.addEventListener("change", () => {
        state.source = sourceSelect.value;
        populateSymbols();
        reloadPanel(state);
    });
    symbolSelect.addEventListener("change", () => {
        state.symbol = symbolSelect.value;
        reloadPanel(state);
    });
    timeSelect.addEventListener("change", () => {
        state.timeframe = timeSelect.value;
        reloadPanel(state);
    });
    indicatorBtn.addEventListener("click", () => panel.classList.toggle("drawer-open"));

    createChart(state);
    reloadPanel(state);
    state.livePoll = setInterval(() => fetchLivePrice(state), 4000);
}

function createChart(state) {
    state.chart = LightweightCharts.createChart(state.chartContainer, {
        autoSize: true,
        layout: { background: { color: "#0e1322" }, textColor: "#b9c1d3" },
        grid: {
            vertLines: { color: "rgba(79, 91, 125, 0.22)" },
            horzLines: { color: "rgba(79, 91, 125, 0.22)" },
        },
        crosshair: {
            vertLine: { color: "rgba(185, 193, 211, 0.45)", style: 2 },
            horzLine: { color: "rgba(185, 193, 211, 0.45)", style: 2 },
        },
        rightPriceScale: { borderColor: "rgba(79, 91, 125, 0.35)" },
        timeScale: { borderColor: "rgba(79, 91, 125, 0.35)", timeVisible: true, secondsVisible: false },
    });
    state.series = state.chart.addCandlestickSeries({
        upColor: "#21c997",
        downColor: "#f24f63",
        borderVisible: false,
        wickUpColor: "#21c997",
        wickDownColor: "#f24f63",
    });
    state.volumeSeries = state.chart.addHistogramSeries({
        priceScaleId: "volume",
        priceFormat: { type: "volume" },
        color: "rgba(100, 116, 139, 0.55)",
    });
    state.chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
}

function setVolume(state) {
    const active = state.indicators.volume !== false;
    state.volumeSeries.setData(active ? state.bars.map((bar) => ({
        time: bar.time,
        value: bar.volume || 0,
        color: bar.close >= bar.open ? "rgba(33, 201, 151, 0.32)" : "rgba(242, 79, 99, 0.32)",
    })) : []);
}

function clearIndicators(state) {
    state.indicatorSeries.forEach((series) => state.chart.removeSeries(series));
    state.indicatorSeries = [];
}

function applyIndicators(state) {
    clearIndicators(state);
    setVolume(state);
    indicatorGroups.flatMap((group) => group.items).forEach((spec) => {
        if (!state.indicators[spec.id] || spec.type === "volume") return;
        const dataSets = indicatorSeriesData(state.bars, spec);
        dataSets.forEach((entry, offset) => {
            const data = Array.isArray(entry) ? entry : entry.data;
            if (!data.length) return;
            const isVolumeProfile = spec.type === "volumeProfile";
            const isFvg = spec.type === "fvg";
            const options = Array.isArray(entry) ? {} : entry;
            const series = state.chart.addLineSeries({
                color: options.color || spec.color,
                lineWidth: options.lineWidth || (isVolumeProfile && offset === 0 ? 3 : offset === 0 ? 2 : 1),
                lineStyle: options.lineStyle ?? (isFvg ? 0 : isVolumeProfile && offset > 0 ? 1 : offset === 1 ? 2 : 0),
                priceLineVisible: false,
                lastValueVisible: options.lastValueVisible ?? isVolumeProfile,
                title: options.title ?? (isVolumeProfile ? ["POC", "VAH", "VAL"][offset] : ""),
            });
            series.setData(data);
            state.indicatorSeries.push(series);
        });
    });
}

function updatePriceDisplay(state, price) {
    const value = Number(price);
    if (!Number.isFinite(value)) return;
    const prevPrice = Number(state.priceElement.dataset.raw || state.lastPrice || value);
    const change = value - prevPrice;
    const changePct = prevPrice ? (change / prevPrice) * 100 : 0;
    state.priceElement.dataset.raw = String(value);
    state.priceElement.textContent = formatPrice(value);
    state.changeElement.textContent = `${change >= 0 ? "+" : ""}${formatPrice(change)} (${changePct >= 0 ? "+" : ""}${changePct.toFixed(2)}%)`;
    state.changeElement.className = change < 0 ? "negative" : "positive";
}

async function reloadPanel(state) {
    const url = `/api/chart_data?source=${encodeURIComponent(state.source)}&symbol=${encodeURIComponent(state.symbol)}&timeframe=${encodeURIComponent(state.timeframe)}`;
    state.panel.classList.add("loading");
    try {
        const res = await fetch(url);
        const data = await res.json();
        state.bars = data.bars || [];
        state.series.setData(state.bars);
        applyIndicators(state);
        state.chart.timeScale().fitContent();
        state.symbolElement.textContent = state.symbol.replace(".NS", "");
        if (state.bars.length) {
            const last = state.bars[state.bars.length - 1];
            state.lastPrice = last.close;
            state.priceElement.dataset.raw = String(last.close);
            state.priceElement.textContent = formatPrice(last.close);
            state.changeElement.textContent = "+0.00 (+0.00%)";
            state.changeElement.className = "positive";
        }
    } catch (error) {
        console.error("Failed loading chart data", state.source, state.symbol, state.timeframe, error);
        state.symbolElement.textContent = state.symbol;
        state.priceElement.textContent = "n/a";
        state.changeElement.textContent = "could not load";
        state.changeElement.className = "negative";
    } finally {
        state.panel.classList.remove("loading");
    }
}

async function fetchLivePrice(state) {
    try {
        const res = await fetch(`/api/live_price?source=${encodeURIComponent(state.source)}&symbol=${encodeURIComponent(state.symbol)}`);
        const data = await res.json();
        if (!data || data.price == null) return;
        updatePriceDisplay(state, data.price);
        updateLiveCandle(state, data);
    } catch {
        // The streaming path remains primary; polling is only a selected-symbol fallback.
    }
}

function updateLiveCandle(state, parsed) {
    const price = Number(parsed.price);
    if (!Number.isFinite(price)) return;
    const time = candleBucket(Number(parsed.timestamp || Date.now() / 1000), state.timeframe);
    const volume = Number(parsed.volume || 0);
    const last = state.bars[state.bars.length - 1];
    let candle;
    if (last && last.time === time) {
        candle = {
            ...last,
            high: Math.max(last.high, price),
            low: Math.min(last.low, price),
            close: price,
            volume: (last.volume || 0) + volume,
        };
        state.bars[state.bars.length - 1] = candle;
    } else {
        candle = { time, open: last ? last.close : price, high: price, low: price, close: price, volume };
        state.bars.push(candle);
        if (state.bars.length > 1500) state.bars.shift();
    }
    state.series.update(candle);
    setVolume(state);
}

function handleMarketEvent(event) {
    const parsed = JSON.parse(event.data);
    paneStates.forEach((state) => {
        if (state.source !== parsed.source) return;
        if (normalizeSymbol(state.source, state.symbol) !== normalizeSymbol(parsed.source, parsed.symbol)) return;
        const previous = Number(state.priceElement.dataset.raw || 0);
        updatePriceDisplay(state, parsed.price);
        updateLiveCandle(state, parsed);
        state.panel.classList.remove("flash-green", "flash-red");
        void state.panel.offsetWidth;
        state.panel.classList.add(Number(parsed.price) >= previous ? "flash-green" : "flash-red");
    });
}

function startEventSource() {
    if (eventSource) eventSource.close();
    try {
        eventSource = new EventSource(`/stream/market?cache=${Date.now()}`);
        eventSource.onmessage = handleMarketEvent;
        eventSource.onerror = () => {
            eventSource.close();
            setTimeout(startEventSource, 2000);
        };
    } catch {
        setTimeout(startEventSource, 2000);
    }
}

function updateLayout(count) {
    const rule = layoutRules[count] || layoutRules[4];
    chartGrid.style.gridTemplateColumns = `repeat(${rule.columns}, minmax(0, 1fr))`;
    chartCountSelect.value = String(count);
    localStorage.setItem(storageKey, String(count));
}

function rebuildPanels(count) {
    paneStates.forEach((state) => {
        clearIndicators(state);
        if (state.livePoll) clearInterval(state.livePoll);
        if (state.chart) state.chart.remove();
    });
    paneStates = [];
    chartGrid.innerHTML = "";
    for (let i = 0; i < count; i += 1) buildPanel(i);
    updateLayout(count);
}

async function init() {
    await loadConfig();
    await refreshMarketStrip();
    setInterval(refreshMarketStrip, 30000);
    const count = getStoredCount();
    rebuildPanels(count);
    chartCountSelect.addEventListener("change", (event) => rebuildPanels(Number(event.target.value)));
    startEventSource();
}

init();

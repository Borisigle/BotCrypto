(() => {
  "use strict";

  const chartRegistry = new Map();
  const signalRows = new Map();
  const currentFilters = { symbol: "", confidence: "", session: "" };
  let filtersInitialised = false;
  let eventSource = null;
  let reconnectTimer = null;
  const MAX_SIGNAL_ROWS = 32;

  const chartContainers = document.querySelectorAll("[data-market-chart]");
  const signalTableBody = document.querySelector("[data-signal-feed-body]");
  const filtersForm = document.getElementById("signal-feed-filters");

  if (!chartContainers.length || !signalTableBody || !filtersForm) {
    return;
  }

  function isoToSeconds(iso) {
    return Math.floor(new Date(iso).getTime() / 1000);
  }

  function formatNumber(value, fractionDigits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return "--";
    }
    return Number(value).toLocaleString(undefined, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    });
  }

  function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return "--";
    }
    return `${formatNumber(Number(value) * 100, 2)}%`;
  }

  function formatTime(iso) {
    const date = new Date(iso);
    return (
      date.toLocaleString(undefined, {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }) +
      " · " +
      date.toLocaleDateString()
    );
  }

  function updateMarketStats(symbol, price, delta, cvd, timeIso) {
    const priceEl = document.querySelector(`[data-market-stat="${symbol}-price"]`);
    const deltaEl = document.querySelector(`[data-market-stat="${symbol}-delta"]`);
    const cvdEl = document.querySelector(`[data-market-stat="${symbol}-cvd"]`);
    const updatedEl = document.querySelector(`[data-market-updated="${symbol}"]`);

    if (priceEl) {
      priceEl.textContent = formatNumber(price);
    }
    if (deltaEl) {
      deltaEl.textContent = formatPercent(delta);
    }
    if (cvdEl) {
      cvdEl.textContent = formatNumber(cvd, 1);
    }
    if (updatedEl && timeIso) {
      updatedEl.textContent = formatTime(timeIso);
    }
  }

  function updateVolumeLevels(symbol, volumeLevels) {
    const vahEl = document.querySelector(
      `[data-market-level][data-symbol="${symbol}"][data-role="vah"]`
    );
    const valEl = document.querySelector(
      `[data-market-level][data-symbol="${symbol}"][data-role="val"]`
    );
    const pocEl = document.querySelector(
      `[data-market-level][data-symbol="${symbol}"][data-role="poc"]`
    );
    const lvnEl = document.querySelector(
      `[data-market-level][data-symbol="${symbol}"][data-role="lvn"]`
    );

    if (vahEl) {
      vahEl.textContent = formatNumber(volumeLevels?.vah);
    }
    if (valEl) {
      valEl.textContent = formatNumber(volumeLevels?.val);
    }
    if (pocEl) {
      pocEl.textContent = formatNumber(volumeLevels?.poc);
    }
    if (lvnEl) {
      const lvns = Array.isArray(volumeLevels?.lvns)
        ? volumeLevels.lvns.map((level) => formatNumber(level)).join(" · ")
        : "--";
      lvnEl.textContent = lvns || "--";
    }
  }

  const resizeObserver = new ResizeObserver((entries) => {
    for (const entry of entries) {
      const symbol = entry.target.dataset.marketChart;
      const registryItem = chartRegistry.get(symbol);
      if (registryItem) {
        registryItem.chart.applyOptions({ width: entry.contentRect.width });
      }
    }
  });

  function createChartForMarket(market) {
    if (!window.LightweightCharts) {
      return;
    }

    const container = document.querySelector(
      `[data-market-chart="${market.symbol}"]`
    );
    if (!container) {
      return;
    }

    const chart = LightweightCharts.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { color: "transparent" },
        textColor: "#cbd5f5",
        fontSize: 12,
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.1)" },
        horzLines: { color: "rgba(148, 163, 184, 0.1)" },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
      },
      timeScale: {
        borderVisible: false,
        rightOffset: 8,
        barSpacing: 14,
      },
      rightPriceScale: {
        borderVisible: false,
      },
    });

    const priceSeries = chart.addAreaSeries({
      topColor: "rgba(76, 139, 253, 0.45)",
      bottomColor: "rgba(76, 139, 253, 0.05)",
      lineColor: "#4c8bfd",
      lineWidth: 2,
    });

    const cvdSeries = chart.addLineSeries({
      color: "#4fd1c5",
      lineWidth: 2,
      priceScaleId: "cvd",
    });
    chart.priceScale("cvd").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0.02 },
      alignLabels: false,
      borderVisible: false,
    });

    const deltaSeries = chart.addLineSeries({
      color: "#f97316",
      lineWidth: 2,
      priceScaleId: "delta",
    });
    chart.priceScale("delta").applyOptions({
      scaleMargins: { top: 0.15, bottom: 0.6 },
      alignLabels: true,
      borderVisible: false,
    });

    const priceData = market.price.map((point) => ({
      time: isoToSeconds(point.time),
      value: point.close,
    }));
    const cvdData = market.cvd.map((point) => ({
      time: isoToSeconds(point.time),
      value: point.value,
    }));
    const deltaData = market.delta_oi_pct.map((point) => ({
      time: isoToSeconds(point.time),
      value: point.value,
    }));

    priceSeries.setData(priceData);
    cvdSeries.setData(cvdData);
    deltaSeries.setData(deltaData);

    chart.timeScale().fitContent();

    const priceLines = [
      {
        price: market.volume_levels?.vah,
        color: "#34d399",
        title: "VAH",
        style: LightweightCharts.LineStyle.Solid,
      },
      {
        price: market.volume_levels?.val,
        color: "#38bdf8",
        title: "VAL",
        style: LightweightCharts.LineStyle.Solid,
      },
      {
        price: market.volume_levels?.poc,
        color: "#f87171",
        title: "POC",
        style: LightweightCharts.LineStyle.Dashed,
      },
    ];

    if (Array.isArray(market.volume_levels?.lvns)) {
      market.volume_levels.lvns.forEach((level, index) => {
        priceLines.push({
          price: level,
          color: "#a78bfa",
          title: `LVN ${index + 1}`,
          style: LightweightCharts.LineStyle.Dotted,
        });
      });
    }

    priceLines
      .filter((line) => typeof line.price === "number")
      .forEach((line) => {
        priceSeries.createPriceLine({
          price: line.price,
          color: line.color,
          lineWidth: 1,
          lineStyle: line.style,
          axisLabelVisible: true,
          title: `${line.title} ${formatNumber(line.price)}`,
        });
      });

    const markers = Array.isArray(market.entry_zones)
      ? market.entry_zones.map((zone) => ({
          time: isoToSeconds(zone.start),
          position: "belowBar",
          color:
            (zone.tier || "").toLowerCase() === "high" ? "#facc15" : "#38bdf8",
          shape: "arrowUp",
          text: (zone.label || "Entry").slice(0, 14),
        }))
      : [];

    if (markers.length) {
      priceSeries.setMarkers(markers);
    }

    chartRegistry.set(market.symbol, {
      chart,
      container,
      priceSeries,
      cvdSeries,
      deltaSeries,
      markers,
    });
    resizeObserver.observe(container);

    const lastPricePoint = market.price[market.price.length - 1];
    const lastDeltaPoint = market.delta_oi_pct[market.delta_oi_pct.length - 1];
    const lastCvdPoint = market.cvd[market.cvd.length - 1];

    updateMarketStats(
      market.symbol,
      lastPricePoint?.close,
      lastDeltaPoint?.value,
      lastCvdPoint?.value,
      lastPricePoint?.time
    );
    updateVolumeLevels(market.symbol, market.volume_levels);
  }

  function populateFilters(filters) {
    if (filtersInitialised) {
      return;
    }

    const symbolSelect = filtersForm.querySelector('[data-filter="symbol"]');
    const confidenceSelect = filtersForm.querySelector('[data-filter="confidence"]');
    const sessionSelect = filtersForm.querySelector('[data-filter="session"]');

    if (symbolSelect) {
      filters.symbols.forEach((symbol) => {
        const option = document.createElement("option");
        option.value = symbol;
        option.textContent = symbol;
        symbolSelect.appendChild(option);
      });
    }

    if (confidenceSelect) {
      filters.confidences.forEach((confidence) => {
        const option = document.createElement("option");
        option.value = confidence;
        option.textContent = confidence;
        confidenceSelect.appendChild(option);
      });
    }

    if (sessionSelect) {
      filters.sessions.forEach((session) => {
        const option = document.createElement("option");
        option.value = session;
        option.textContent = session.replace(/_/g, " ");
        sessionSelect.appendChild(option);
      });
    }

    filtersInitialised = true;
  }

  function clearSignalTable() {
    signalRows.clear();
    signalTableBody.innerHTML = "";
  }

  function upsertSignalRow(signal, { prepend = false } = {}) {
    const matchesSymbol =
      !currentFilters.symbol || currentFilters.symbol === signal.symbol;
    const matchesConfidence =
      !currentFilters.confidence || currentFilters.confidence === signal.confidence;
    const matchesSession =
      !currentFilters.session || currentFilters.session === signal.session;

    if (!matchesSymbol || !matchesConfidence || !matchesSession) {
      return;
    }

    const emptyRow = signalTableBody.querySelector(".empty");
    if (emptyRow) {
      emptyRow.remove();
    }

    let row = signalRows.get(signal.id);
    const isNewRow = !row;

    if (!row) {
      row = document.createElement("tr");
      row.dataset.signalId = String(signal.id);
      signalRows.set(signal.id, row);
    }

    const deltaDisplay = formatPercent(signal.delta_oi_pct);
    const cvdDisplay = formatNumber(signal.cvd, 1);
    const entryDisplay = formatNumber(signal.entry_price);

    row.innerHTML = `
      <td>#${signal.id}</td>
      <td>${formatTime(signal.generated_at)}</td>
      <td>${signal.symbol}</td>
      <td>${signal.confidence || "--"}</td>
      <td>${signal.session || "--"}</td>
      <td>${signal.tier || "--"}</td>
      <td>${deltaDisplay}</td>
      <td>${cvdDisplay}</td>
      <td>${entryDisplay}</td>
      <td>${signal.status || "--"}</td>
      <td>${signal.notes || ""}</td>
    `;

    if (isNewRow) {
      if (prepend) {
        signalTableBody.prepend(row);
      } else {
        signalTableBody.appendChild(row);
      }
    }

    trimTableRows();
  }

  function renderSignals(signals) {
    clearSignalTable();

    if (!signals.length) {
      const emptyRow = document.createElement("tr");
      emptyRow.classList.add("empty");
      emptyRow.innerHTML = `<td colspan="11">No signals match the current filters.</td>`;
      signalTableBody.appendChild(emptyRow);
      return;
    }

    signals.forEach((signal) => {
      upsertSignalRow(signal, { prepend: false });
    });
  }

  function trimTableRows() {
    const rows = Array.from(signalTableBody.querySelectorAll("tr"));
    rows
      .filter((row) => !row.classList.contains("empty"))
      .slice(MAX_SIGNAL_ROWS)
      .forEach((row) => {
        signalRows.delete(Number(row.dataset.signalId));
        row.remove();
      });
  }

  async function loadMarkets() {
    try {
      const response = await fetch("/api/v1/markets");
      if (!response.ok) {
        throw new Error(`Failed to load markets: ${response.status}`);
      }
      const payload = await response.json();
      if (!payload?.markets) {
        return;
      }
      payload.markets.forEach((market) => {
        createChartForMarket(market);
      });
    } catch (error) {
      console.error(error);
    }
  }

  async function loadSignalFeed(params = {}) {
    const url = new URL("/api/v1/signals/feed", window.location.origin);
    ["symbol", "confidence", "session"].forEach((key) => {
      const value = params[key];
      if (value) {
        url.searchParams.set(key, value);
      }
    });

    try {
      const response = await fetch(url.toString());
      if (!response.ok) {
        throw new Error(`Failed to load signal feed: ${response.status}`);
      }
      const payload = await response.json();
      if (!filtersInitialised && payload.filters) {
        populateFilters(payload.filters);
      }
      renderSignals(payload.signals || []);
    } catch (error) {
      console.error(error);
    }
  }

  function handleFilterChange(event) {
    if (event.target.tagName !== "SELECT") {
      return;
    }

    currentFilters.symbol = filtersForm
      .querySelector('[data-filter="symbol"]')
      .value.trim();
    currentFilters.confidence = filtersForm
      .querySelector('[data-filter="confidence"]')
      .value.trim();
    currentFilters.session = filtersForm
      .querySelector('[data-filter="session"]')
      .value.trim();

    loadSignalFeed(currentFilters);
  }

  function updateChartsFromSignal(signal) {
    if (!signal?.market_point) {
      return;
    }

    const registryItem = chartRegistry.get(signal.symbol);
    if (!registryItem) {
      return;
    }

    const { market_point: marketPoint } = signal;

    const pricePoint = {
      time: isoToSeconds(marketPoint.price.time),
      value: marketPoint.price.close,
    };
    const cvdPoint = {
      time: isoToSeconds(marketPoint.cvd.time),
      value: marketPoint.cvd.value,
    };
    const deltaPoint = {
      time: isoToSeconds(marketPoint.delta_oi_pct.time),
      value: marketPoint.delta_oi_pct.value,
    };

    registryItem.priceSeries.update(pricePoint);
    registryItem.cvdSeries.update(cvdPoint);
    registryItem.deltaSeries.update(deltaPoint);

    const markerColor =
      (signal.tier || "").toLowerCase() === "high" ? "#facc15" : "#38bdf8";
    const marker = {
      time: pricePoint.time,
      position: "belowBar",
      color: markerColor,
      shape: "arrowUp",
      text: (signal.notes || signal.tier || "Entry").slice(0, 14),
    };
    registryItem.markers.push(marker);
    registryItem.priceSeries.setMarkers(registryItem.markers.slice(-8));

    updateMarketStats(
      signal.symbol,
      marketPoint.price.close,
      marketPoint.delta_oi_pct.value,
      marketPoint.cvd.value,
      marketPoint.price.time
    );
  }

  function startSignalStream() {
    if (eventSource) {
      eventSource.close();
    }
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
    }

    eventSource = new EventSource("/api/v1/signals/stream");

    eventSource.addEventListener("signal", (event) => {
      try {
        const payload = JSON.parse(event.data || "{}");
        if (!payload?.signal) {
          return;
        }
        updateChartsFromSignal(payload.signal);
        upsertSignalRow(payload.signal, { prepend: true });
      } catch (error) {
        console.error("Failed to parse SSE payload", error);
      }
    });

    eventSource.addEventListener("error", () => {
      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
      reconnectTimer = setTimeout(startSignalStream, 5000);
    });
  }

  filtersForm.addEventListener("change", handleFilterChange);

  loadMarkets();
  loadSignalFeed(currentFilters);
  startSignalStream();
})();

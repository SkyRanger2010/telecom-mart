/**
 * Линейные KPI-графики в стиле дашборда «Абонентская база»: оси, сетка, переключатели рядов.
 */
(function (global) {
  "use strict";

  function slugKey(label, idx) {
    var s = String(label == null ? "" : label)
      .replace(/\s+/g, "_")
      .replace(/[^\w\u0400-\u04FF-]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 40);
    return "s" + idx + "_" + (s || "series");
  }

  function getLayoutParams(readWidth) {
    var w = readWidth || global.innerWidth || 1280;
    var h = global.innerHeight || 800;
    var base;
    if (w >= 1520) {
      base = { lineBorder: 2.5, pointRadius: 4, pointHoverRadius: 7, pointBorderWidth: 2, maxTicksX: 18, maxTicksY: 12 };
    } else if (w >= 1200) {
      base = { lineBorder: 2, pointRadius: 3, pointHoverRadius: 6, pointBorderWidth: 1, maxTicksX: 14, maxTicksY: 10 };
    } else if (w >= 900) {
      base = { lineBorder: 1.5, pointRadius: 2, pointHoverRadius: 5, pointBorderWidth: 1, maxTicksX: 11, maxTicksY: 8 };
    } else {
      base = { lineBorder: 1, pointRadius: 1.5, pointHoverRadius: 4, pointBorderWidth: 1, maxTicksX: 8, maxTicksY: 6 };
    }
    if (h < 820) {
      base.maxTicksX = Math.max(6, base.maxTicksX - 3);
      base.maxTicksY = Math.max(5, base.maxTicksY - 2);
      base.lineBorder = Math.max(1, base.lineBorder - 0.5);
      base.pointRadius = Math.max(0, base.pointRadius - 0.5);
    }
    if (h < 700) {
      base.maxTicksX = Math.max(5, base.maxTicksX - 2);
      base.maxTicksY = Math.max(4, base.maxTicksY - 1);
    }
    return base;
  }

  function normalizeSpec(spec) {
    if (!spec) return spec;
    (spec.datasets || []).forEach(function (ds, i) {
      if (!ds.key) ds.key = slugKey(ds.label, i);
    });
    return spec;
  }

  function ensureSeriesDefaults(spec, visibleSet) {
    if (!spec || !visibleSet || visibleSet.size > 0) return;
    (spec.datasets || []).forEach(function (ds, i) {
      visibleSet.add(ds.key || slugKey(ds.label, i));
    });
  }

  function isSeriesVisible(visibleSet, key) {
    if (!visibleSet || visibleSet.size === 0) return true;
    return visibleSet.has(key);
  }

  function mapLineDatasets(spec, visibleSet, layout) {
    var p = layout || getLayoutParams();
    return (spec.datasets || []).map(function (d, i) {
      var key = d.key || slugKey(d.label, i);
      var border = d.borderColor || "#888";
      var data = (d.data || []).map(function (v) {
        if (v === null || v === undefined) return 0;
        var n = Number(v);
        return isNaN(n) ? 0 : n;
      });
      return {
        label: d.label,
        data: data,
        borderColor: border,
        backgroundColor: d.backgroundColor || border,
        borderWidth: typeof d.borderWidth === "number" ? d.borderWidth : p.lineBorder,
        fill: false,
        tension: typeof d.tension === "number" ? d.tension : 0.15,
        spanGaps: false,
        pointRadius: typeof d.pointRadius === "number" ? d.pointRadius : p.pointRadius,
        pointHoverRadius: typeof d.pointHoverRadius === "number" ? d.pointHoverRadius : p.pointHoverRadius,
        pointHitRadius: typeof d.pointHitRadius === "number" ? d.pointHitRadius : 8,
        pointBackgroundColor: d.pointBackgroundColor || border,
        pointBorderColor: d.pointBorderColor || border,
        pointBorderWidth: typeof d.pointBorderWidth === "number" ? d.pointBorderWidth : p.pointBorderWidth,
        hidden: !isSeriesVisible(visibleSet, key),
      };
    });
  }

  function chartOptions(spec, layout) {
    var p = layout || getLayoutParams();
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        title: { display: false },
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              var v = ctx.parsed.y;
              if (v == null || isNaN(v)) return ctx.dataset.label + ": 0";
              return ctx.dataset.label + ": " + Math.round(v);
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: !!(spec && spec.xlabel), text: (spec && spec.xlabel) || "", color: "#8b9cb3" },
          ticks: { color: "#8b9cb3", maxRotation: 45, autoSkip: true, maxTicksLimit: p.maxTicksX },
          grid: { color: "rgba(45, 58, 77, 0.55)" },
        },
        y: {
          title: { display: !!(spec && spec.ylabel), text: (spec && spec.ylabel) || "", color: "#8b9cb3" },
          ticks: { color: "#8b9cb3", autoSkip: true, maxTicksLimit: p.maxTicksY },
          beginAtZero: true,
          grid: { color: "rgba(45, 58, 77, 0.55)" },
        },
      },
    };
  }

  function renderSeriesToggles(containerEl, spec, chart, visibleSet) {
    if (!containerEl) return;
    containerEl.innerHTML = "";
    normalizeSpec(spec);
    (spec.datasets || []).forEach(function (ds, idx) {
      var key = ds.key || slugKey(ds.label, idx);
      var lbl = document.createElement("label");
      lbl.className = "series-toggle-row";
      var cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = isSeriesVisible(visibleSet, key);
      cb.dataset.key = key;
      cb.dataset.index = String(idx);
      var sw = document.createElement("span");
      sw.className = "series-swatch";
      sw.style.background = ds.borderColor || "#888";
      lbl.appendChild(cb);
      lbl.appendChild(sw);
      lbl.appendChild(document.createTextNode(ds.label || key));
      containerEl.appendChild(lbl);
    });
    containerEl.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        var key = cb.dataset.key;
        if (cb.checked) visibleSet.add(key);
        else visibleSet.delete(key);
        var idx = parseInt(cb.dataset.index, 10);
        if (chart && !isNaN(idx)) {
          chart.setDatasetVisibility(idx, cb.checked);
          chart.update();
        }
      });
    });
  }

  function applyResponsiveLayout(chart, readWidthFn) {
    if (!chart || !chart.ctx) return;
    var w = readWidthFn ? readWidthFn() : null;
    var p = getLayoutParams(w);
    try {
      if (chart.options && chart.options.scales) {
        if (chart.options.scales.x && chart.options.scales.x.ticks) {
          chart.options.scales.x.ticks.maxTicksLimit = p.maxTicksX;
        }
        if (chart.options.scales.y && chart.options.scales.y.ticks) {
          chart.options.scales.y.ticks.maxTicksLimit = p.maxTicksY;
        }
      }
      (chart.data.datasets || []).forEach(function (ds) {
        ds.borderWidth = p.lineBorder;
        ds.pointRadius = p.pointRadius;
        ds.pointHoverRadius = p.pointHoverRadius;
        ds.pointBorderWidth = p.pointBorderWidth;
      });
      chart.update("none");
    } catch (e) { /* ignore */ }
  }

  function create(canvas, spec, visibleSet, readWidthFn) {
    if (!canvas || typeof global.Chart === "undefined" || !spec || !spec.labels || !spec.labels.length) {
      return null;
    }
    normalizeSpec(spec);
    var set = visibleSet || new Set();
    ensureSeriesDefaults(spec, set);
    var layout = getLayoutParams(readWidthFn ? readWidthFn() : null);
    var datasets = mapLineDatasets(spec, set, layout);
    if (!datasets.length) return null;
    var chart = new global.Chart(canvas, {
      type: "line",
      data: { labels: spec.labels, datasets: datasets },
      options: chartOptions(spec, layout),
    });
    return chart;
  }

  function mount(opts) {
    opts = opts || {};
    if (opts.existingChart) {
      try { opts.existingChart.destroy(); } catch (e) { /* ignore */ }
    }
    var visibleSet = opts.visibleSet || new Set();
    var chart = create(opts.canvas, opts.spec, visibleSet, opts.readWidth);
    if (opts.togglesEl && chart) {
      renderSeriesToggles(opts.togglesEl, opts.spec, chart, visibleSet);
    }
    if (chart && opts.onMounted) opts.onMounted(chart, visibleSet);
    return { chart: chart, visibleSet: visibleSet };
  }

  global.TmKpiLineChart = {
    mount: mount,
    create: create,
    destroy: function (chart) {
      if (chart) try { chart.destroy(); } catch (e) { /* ignore */ }
    },
    renderSeriesToggles: renderSeriesToggles,
    ensureSeriesDefaults: ensureSeriesDefaults,
    applyResponsiveLayout: applyResponsiveLayout,
    getLayoutParams: getLayoutParams,
    normalizeSpec: normalizeSpec,
  };
})(typeof window !== "undefined" ? window : this);

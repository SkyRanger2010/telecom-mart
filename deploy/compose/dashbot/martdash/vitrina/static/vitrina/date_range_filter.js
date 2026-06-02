/**
 * Быстрые пресеты диапазона дат для дашбордов (flatpickr + скрытые df/dt).
 */
(function (global) {
  "use strict";

  var DEFAULT_PRESET_IDS = ["7d", "30d", "90d", "mtd", "prev_month", "all"];

  function parseYmd(s) {
    if (!s) return null;
    var p = String(s).trim().split("-");
    if (p.length !== 3) return null;
    var y = parseInt(p[0], 10);
    var m = parseInt(p[1], 10) - 1;
    var d = parseInt(p[2], 10);
    if (isNaN(y) || isNaN(m) || isNaN(d)) return null;
    return new Date(y, m, d);
  }

  function formatYmd(d) {
    if (!d || typeof flatpickr === "undefined") return "";
    return flatpickr.formatDate(d, "Y-m-d");
  }

  function addDays(d, n) {
    var x = new Date(d.getTime());
    x.setDate(x.getDate() + n);
    return x;
  }

  function startOfMonth(d) {
    return new Date(d.getFullYear(), d.getMonth(), 1);
  }

  function endOfMonth(d) {
    return new Date(d.getFullYear(), d.getMonth() + 1, 0);
  }

  function clampDate(d, lo, hi) {
    if (!d) return d;
    if (lo && d < lo) return new Date(lo.getTime());
    if (hi && d > hi) return new Date(hi.getTime());
    return d;
  }

  function computePreset(id, bounds) {
    bounds = bounds || {};
    var maxD = parseYmd(bounds.max) || new Date();
    var minD = parseYmd(bounds.min);
    maxD = new Date(maxD.getFullYear(), maxD.getMonth(), maxD.getDate());
    var today = maxD;
    var from;
    var to = today;

    switch (id) {
      case "7d":
        from = addDays(today, -6);
        break;
      case "30d":
        from = addDays(today, -29);
        break;
      case "90d":
        from = addDays(today, -89);
        break;
      case "mtd":
        from = startOfMonth(today);
        break;
      case "prev_month": {
        var pm = new Date(today.getFullYear(), today.getMonth() - 1, 1);
        from = pm;
        to = endOfMonth(pm);
        break;
      }
      case "ytd":
        from = new Date(today.getFullYear(), 0, 1);
        break;
      case "all":
        from = minD ? new Date(minD.getTime()) : addDays(today, -89);
        to = today;
        break;
      default:
        return null;
    }

    if (minD) from = clampDate(from, minD, null);
    to = clampDate(to, minD, maxD);
    from = clampDate(from, minD, to);
    if (from > to) from = new Date(to.getTime());
    return { from: from, to: to };
  }

  function defaultLabels() {
    return {
      "7d": "7 дней",
      "30d": "30 дней",
      "90d": "90 дней",
      mtd: "Месяц",
      prev_month: "Пр. месяц",
      ytd: "С года",
      all: "Весь период",
      aria: "Быстрый выбор периода",
    };
  }

  function applyRange(opts, fromDate, toDate) {
    var df = document.getElementById(opts.dfId || "id_df");
    var dt = document.getElementById(opts.dtId || "id_dt");
    if (!df || !dt || !fromDate || !toDate) return;
    var d0 = formatYmd(fromDate);
    var d1 = formatYmd(toDate);
    if (!d0 || !d1) return;
    df.value = d0;
    dt.value = d1;
    var fp = opts.fpInstance;
    if (fp && typeof fp.setDate === "function") {
      fp.setDate([fromDate, toDate], true);
    }
    if (opts.quickContainerId) {
      clearActiveQuick(opts.quickContainerId);
      if (opts._activePresetId) {
        var btn = document.querySelector(
          "#" + opts.quickContainerId + ' [data-range-preset="' + opts._activePresetId + '"]'
        );
        if (btn) btn.classList.add("active");
      }
    }
    if (opts.onChange) opts.onChange(d0, d1);
  }

  function clearActiveQuick(containerId) {
    var wrap = document.getElementById(containerId);
    if (!wrap) return;
    wrap.querySelectorAll(".filter-quick-pick.active").forEach(function (b) {
      b.classList.remove("active");
    });
  }

  function mountQuickPicks(opts) {
    var wrap = document.getElementById(opts.quickContainerId);
    if (!wrap) return;
    var labels = opts.labels || defaultLabels();
    var ids = opts.presetIds || DEFAULT_PRESET_IDS;
    wrap.innerHTML = "";
    wrap.className = "filter-date-quick";
    wrap.setAttribute("role", "group");
    wrap.setAttribute("aria-label", labels.aria || defaultLabels().aria);

    ids.forEach(function (id) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "filter-quick-pick";
      btn.setAttribute("data-range-preset", id);
      btn.textContent = labels[id] || id;
      wrap.appendChild(btn);
    });

    wrap.addEventListener("click", function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      var btn = t.closest("[data-range-preset]");
      if (!btn) return;
      var presetId = btn.getAttribute("data-range-preset");
      var range = computePreset(presetId, opts.bounds);
      if (!range) return;
      opts._activePresetId = presetId;
      applyRange(opts, range.from, range.to);
    });
  }

  function init(opts) {
    opts = opts || {};
    var picker = document.getElementById(opts.pickerId || "id_range_picker");
    var df = document.getElementById(opts.dfId || "id_df");
    var dt = document.getElementById(opts.dtId || "id_dt");
    if (!picker || !df || !dt || typeof flatpickr === "undefined") return null;

    var fp = flatpickr(picker, {
      mode: "range",
      locale: (flatpickr.l10ns && flatpickr.l10ns.ru) ? flatpickr.l10ns.ru : "ru",
      dateFormat: "Y-m-d",
      defaultDate: [df.value, dt.value],
      onChange: function (sel) {
        opts._activePresetId = null;
        if (opts.quickContainerId) clearActiveQuick(opts.quickContainerId);
        if (sel.length === 1) {
          df.value = formatYmd(sel[0]);
          dt.value = df.value;
        } else if (sel.length >= 2) {
          df.value = formatYmd(sel[0]);
          dt.value = formatYmd(sel[sel.length - 1]);
        }
        if (opts.onChange) opts.onChange(df.value, dt.value);
      },
    });

    opts.fpInstance = fp;
    if (opts.quickContainerId) mountQuickPicks(opts);
    return fp;
  }

  global.VitrinaDateRangeFilter = {
    init: init,
    mountQuickPicks: mountQuickPicks,
    applyRange: applyRange,
    clearActive: clearActiveQuick,
    computePreset: computePreset,
    parseYmd: parseYmd,
    formatYmd: formatYmd,
    DEFAULT_PRESET_IDS: DEFAULT_PRESET_IDS,
  };
})(typeof window !== "undefined" ? window : globalThis);

/**
 * Групповой фильтр «Тип клиента»: РФ / Нерезиденты / УКР + быстрые person/org/ip.
 * Значение чекбокса — client_type_title (подпись); data-code — person|org|ip.
 */
(function (global) {
  "use strict";

  function norm(s) {
    return String(s == null ? "" : s).trim();
  }

  function mergeFilterData(data, selectedSet) {
    var base = {
      client_types: (data && data.client_types) || [],
      client_type_groups: (data && data.client_type_groups) || [],
      client_type_quick_picks: (data && data.client_type_quick_picks) || [],
    };
    var seen = {};
    base.client_types.forEach(function (it) {
      seen[norm(it.value)] = it;
    });
    if (selectedSet && selectedSet.forEach) {
      selectedSet.forEach(function (v) {
        v = norm(v);
        if (v && !seen[v]) {
          seen[v] = { value: v, label: v, code: "unknown" };
        }
      });
    }
    var items = Object.keys(seen).map(function (k) {
      return seen[k];
    });
    items.sort(function (a, b) {
      return String(a.label || a.value).localeCompare(String(b.label || b.value), undefined, {
        sensitivity: "base",
      });
    });
    if (!base.client_type_groups.length && items.length) {
      base.client_type_groups = [{ id: "all", label: "", items: items }];
    }
    base.client_types = items;
    return base;
  }

  function groupCheckState(groupEl) {
    var boxes = groupEl.querySelectorAll('.client-type-group-items input[type="checkbox"]');
    var total = 0;
    var checked = 0;
    boxes.forEach(function (cb) {
      total++;
      if (cb.checked) checked++;
    });
    return { total: total, checked: checked };
  }

  function syncGroupHead(groupEl) {
    var head = groupEl.querySelector('input[data-role="group"]');
    if (!head) return;
    var st = groupCheckState(groupEl);
    if (!st.total) {
      head.checked = false;
      head.indeterminate = false;
      return;
    }
    head.checked = st.checked === st.total;
    head.indeterminate = st.checked > 0 && st.checked < st.total;
  }

  function syncAllGroups(box) {
    box.querySelectorAll(".client-type-group").forEach(syncGroupHead);
  }

  function read(containerId, targetSet) {
    targetSet.clear();
    var box = typeof containerId === "string"
      ? document.getElementById(containerId)
      : containerId;
    if (!box || !box.querySelectorAll) return;
    box.querySelectorAll('.client-type-group-items input[type="checkbox"]:checked').forEach(function (cb) {
      var v = norm(cb.value);
      if (v) targetSet.add(v);
    });
  }

  function render(containerId, data, selectedSet, disabled, opts) {
    opts = opts || {};
    var box = document.getElementById(containerId);
    if (!box) return;
    var merged = mergeFilterData(data, selectedSet);
    var groups = merged.client_type_groups || [];
    var quick = merged.client_type_quick_picks || [];
    var msgs = (opts.messages) || {};
    var labels = (opts.labels) || {};
    var off = !!disabled;

    box.innerHTML = "";
    box.classList.toggle("disabled", off);

    if (!groups.length && !(merged.client_types || []).length) {
      var empty = document.createElement("p");
      empty.className = "muted";
      empty.style.fontSize = "0.75rem";
      empty.style.margin = "0.25rem 0";
      empty.textContent = off
        ? msgs.client_type_unavailable || "Недоступно"
        : labels.filter_empty || "Нет значений за период";
      box.appendChild(empty);
      return;
    }

    if (quick.length) {
      var quickRow = document.createElement("div");
      quickRow.className = "filter-client-type-quick";
      quickRow.setAttribute("role", "group");
      quickRow.setAttribute("aria-label", labels.quick_picks_aria || "Быстрый выбор");
      quick.forEach(function (qp) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "filter-quick-pick";
        btn.setAttribute("data-quick-code", qp.code || qp.id || "");
        btn.textContent = qp.label || qp.id || "";
        btn.disabled = off;
        quickRow.appendChild(btn);
      });
      box.appendChild(quickRow);
    }

    var groupsWrap = document.createElement("div");
    groupsWrap.className = "filter-client-type-groups";

    groups.forEach(function (grp) {
      var gEl = document.createElement("div");
      gEl.className = "client-type-group";
      gEl.setAttribute("data-group-id", grp.id || "");

      var head = document.createElement("div");
      head.className = "client-type-group-head";
      var headLbl = document.createElement("label");
      headLbl.className = "filter-check-row client-type-group-label";
      var headCb = document.createElement("input");
      headCb.type = "checkbox";
      headCb.setAttribute("data-role", "group");
      headCb.disabled = off;
      headLbl.appendChild(headCb);
      var headCap = document.createElement("span");
      headCap.className = "client-type-group-title";
      headCap.textContent = grp.label || grp.id || "";
      headLbl.appendChild(headCap);
      head.appendChild(headLbl);
      gEl.appendChild(head);

      var itemsWrap = document.createElement("div");
      itemsWrap.className = "client-type-group-items";
      (grp.items || []).forEach(function (it) {
        var val = norm(it.value);
        var row = document.createElement("label");
        row.className = "filter-check-row client-type-item";
        var cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = val;
        cb.setAttribute("data-code", norm(it.code || ""));
        cb.checked = selectedSet.has(val);
        cb.disabled = off;
        row.appendChild(cb);
        var cap = document.createElement("span");
        cap.className = "filter-check-label-text";
        cap.textContent = it.label || val;
        cap.title = cap.textContent;
        row.appendChild(cap);
        itemsWrap.appendChild(row);
      });
      gEl.appendChild(itemsWrap);
      groupsWrap.appendChild(gEl);
      syncGroupHead(gEl);
    });

    box.appendChild(groupsWrap);
  }

  function bind(containerId, selectedSet, onChange) {
    var box = document.getElementById(containerId);
    if (!box) return;

    box.addEventListener("change", function (e) {
      var t = e.target;
      if (!t || t.tagName !== "INPUT" || t.type !== "checkbox") return;

      if (t.getAttribute("data-role") === "group") {
        var group = t.closest(".client-type-group");
        if (!group) return;
        var on = t.checked;
        group.querySelectorAll('.client-type-group-items input[type="checkbox"]').forEach(function (cb) {
          cb.checked = on;
        });
      }

      read(box, selectedSet);
      syncAllGroups(box);
      if (onChange) onChange();
    });

    box.addEventListener("click", function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      var btn = t.closest(".filter-quick-pick");
      if (!btn || box.classList.contains("disabled")) return;
      var code = norm(btn.getAttribute("data-quick-code")).toLowerCase();
      if (!code) return;
      var matching = [];
      box.querySelectorAll('.client-type-group-items input[type="checkbox"]').forEach(function (cb) {
        if (norm(cb.getAttribute("data-code")).toLowerCase() === code) matching.push(cb);
      });
      if (!matching.length) return;
      var allOn = matching.every(function (cb) {
        return cb.checked;
      });
      matching.forEach(function (cb) {
        cb.checked = !allOn;
      });
      read(box, selectedSet);
      syncAllGroups(box);
      if (onChange) onChange();
    });
  }

  function setAll(containerId, selectedSet, on) {
    var box = document.getElementById(containerId);
    if (!box || box.classList.contains("disabled")) return;
    box.querySelectorAll('.client-type-group-items input[type="checkbox"]').forEach(function (cb) {
      cb.checked = !!on;
    });
    read(box, selectedSet);
    syncAllGroups(box);
  }

  global.VitrinaClientTypeFilter = {
    mergeFilterData: mergeFilterData,
    render: render,
    read: read,
    bind: bind,
    setAll: setAll,
  };
})(typeof window !== "undefined" ? window : globalThis);

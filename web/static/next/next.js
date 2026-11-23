/* FUNBA — Next UI runtime (no dependencies) */
(function () {
  'use strict';
  var FX = (window.FX = window.FX || {});
  var lang = (window.FUNBA && window.FUNBA.lang) || 'en';
  var zh = lang === 'zh';
  var T = function (en, cn) { return zh && cn ? cn : en; };
  FX.t = T;

  var $ = function (sel, root) { return (root || document).querySelector(sel); };
  var $$ = function (sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); };
  FX.$ = $; FX.$$ = $$;
  var SVGNS = 'http://www.w3.org/2000/svg';
  function svgEl(tag, attrs, parent) {
    var el = document.createElementNS(SVGNS, tag);
    if (attrs) for (var k in attrs) if (attrs[k] != null) el.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(el);
    return el;
  }
  FX.svgEl = svgEl;
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  FX.esc = esc;
  function cssVar(name, el) { return getComputedStyle(el || document.documentElement).getPropertyValue(name).trim(); }
  FX.cssVar = cssVar;
  function isDark() { return document.documentElement.getAttribute('data-theme') === 'dark'; }
  // Lighten dark team colors in dark mode so charts stay legible.
  function uiColor(hex) {
    if (!hex || hex[0] !== '#' || !isDark()) return hex;
    var r = parseInt(hex.substr(1, 2), 16), g = parseInt(hex.substr(3, 2), 16), b = parseInt(hex.substr(5, 2), 16);
    var lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
    if (lum > 0.45) return hex;
    var mix = 0.45 - lum + 0.25;
    r = Math.round(r + (255 - r) * mix); g = Math.round(g + (255 - g) * mix); b = Math.round(b + (255 - b) * mix);
    return '#' + [r, g, b].map(function (v) { return ('0' + v.toString(16)).slice(-2); }).join('');
  }
  FX.uiColor = uiColor;

  /* ── Theme ─────────────────────────────────────────────────────────── */
  function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    try { localStorage.setItem('theme', theme); } catch (_) {}
    document.dispatchEvent(new CustomEvent('fx:theme', { detail: theme }));
  }
  document.addEventListener('click', function (e) {
    if (e.target.closest('[data-theme-toggle]')) setTheme(isDark() ? 'light' : 'dark');
    var mt = e.target.closest('[data-menu-toggle]');
    if (mt) {
      var nav = document.getElementById('nav');
      var open = !nav.classList.contains('is-open');
      nav.classList.toggle('is-open', open);
      mt.setAttribute('aria-expanded', open ? 'true' : 'false');
    }
  });

  /* ── Tooltip ───────────────────────────────────────────────────────── */
  var tipEl = null;
  FX.tip = {
    show: function (html, x, y) {
      tipEl = tipEl || $('[data-tip]');
      if (!tipEl) return;
      tipEl.innerHTML = html;
      tipEl.classList.add('is-on');
      var w = tipEl.offsetWidth, h = tipEl.offsetHeight;
      var left = Math.min(window.innerWidth - w - 10, Math.max(10, x + 14));
      var top = y - h - 14;
      if (top < 10) top = y + 18;
      tipEl.style.left = left + 'px';
      tipEl.style.top = top + 'px';
    },
    hide: function () { if (tipEl) tipEl.classList.remove('is-on'); }
  };
  document.addEventListener('mouseover', function (e) {
    var t = e.target.closest('[data-tooltip]');
    if (!t) return;
    var r = t.getBoundingClientRect();
    FX.tip.show(t.getAttribute('data-tooltip'), r.left + r.width / 2 - 14, r.top);
  });
  document.addEventListener('mouseout', function (e) {
    if (e.target.closest('[data-tooltip]')) FX.tip.hide();
  });

  /* ── Reveal on scroll ──────────────────────────────────────────────── */
  function initReveal() {
    var els = $$('.rv');
    if (!('IntersectionObserver' in window)) { els.forEach(function (el) { el.classList.add('is-in'); }); return; }
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) {
        if (en.isIntersecting) { en.target.classList.add('is-in'); io.unobserve(en.target); }
      });
    }, { rootMargin: '0px 0px -8% 0px' });
    els.forEach(function (el) { io.observe(el); });
  }
  FX.onVisible = function (el, cb) {
    if (!('IntersectionObserver' in window)) return cb();
    var io = new IntersectionObserver(function (entries) {
      if (entries[0].isIntersecting) { io.disconnect(); cb(); }
    }, { rootMargin: '0px 0px -5% 0px' });
    io.observe(el);
  };

  /* ── Tabs ──────────────────────────────────────────────────────────── */
  document.addEventListener('click', function (e) {
    var tab = e.target.closest('[data-tab]');
    if (!tab) return;
    var group = tab.closest('[data-tabs]');
    if (!group) return;
    e.preventDefault();
    var key = tab.getAttribute('data-tab');
    var scope = group.getAttribute('data-tabs');
    $$('[data-tab]', group).forEach(function (b) {
      var on = b.getAttribute('data-tab') === key;
      b.classList.toggle('is-on', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    $$('[data-panel-of="' + scope + '"]').forEach(function (p) {
      p.hidden = p.getAttribute('data-panel') !== key;
    });
    document.dispatchEvent(new CustomEvent('fx:tab', { detail: { scope: scope, key: key } }));
  });

  /* ── Sortable tables ───────────────────────────────────────────────── */
  document.addEventListener('click', function (e) {
    var th = e.target.closest('th.sortable');
    if (!th) return;
    var table = th.closest('table');
    var tbody = table.tBodies[0];
    var idx = Array.prototype.indexOf.call(th.parentNode.children, th);
    var asc = th.classList.contains('is-sorted') ? !th.classList.contains('asc') : th.hasAttribute('data-asc-first');
    $$('th', table).forEach(function (h) { h.classList.remove('is-sorted', 'asc'); });
    th.classList.add('is-sorted');
    if (asc) th.classList.add('asc');
    var rows = Array.prototype.slice.call(tbody.rows).filter(function (r) { return !r.hasAttribute('data-nosort'); });
    rows.sort(function (a, b) {
      var av = a.cells[idx] ? (a.cells[idx].getAttribute('data-v') || a.cells[idx].textContent) : '';
      var bv = b.cells[idx] ? (b.cells[idx].getAttribute('data-v') || b.cells[idx].textContent) : '';
      var an = parseFloat(String(av).replace(/[^0-9.\-]/g, '')), bn = parseFloat(String(bv).replace(/[^0-9.\-]/g, ''));
      var cmp;
      if (!isNaN(an) && !isNaN(bn)) cmp = an - bn;
      else if (!isNaN(an)) cmp = 1;
      else if (!isNaN(bn)) cmp = -1;
      else cmp = String(av).localeCompare(String(bv));
      return asc ? cmp : -cmp;
    });
    rows.forEach(function (r) { tbody.appendChild(r); r.classList.remove('group-break'); });
  });

  /* ── Command palette ───────────────────────────────────────────────── */
  function initPalette() {
    var root = $('[data-palette]');
    if (!root) return;
    var input = $('[data-palette-input]', root);
    var results = $('[data-palette-results]', root);
    var initialHTML = results.innerHTML;
    var items = [], active = -1, timer = null, lastQ = '', controller = null;

    function open(prefill) {
      root.classList.add('is-open');
      document.body.style.overflow = 'hidden';
      if (prefill != null) input.value = prefill;
      input.focus();
      setTimeout(function () { input.focus(); input.select(); }, 30);
      if (input.value) search(input.value);
    }
    function close() {
      root.classList.remove('is-open');
      document.body.style.overflow = '';
    }
    function setActive(i) {
      items.forEach(function (it, j) { it.classList.toggle('is-active', j === i); });
      active = i;
      if (items[i]) items[i].scrollIntoView({ block: 'nearest' });
    }
    var icons = {
      metric: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg>',
      ask: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z"/></svg>'
    };
    function render(data) {
      var html = '';
      (data.groups || []).forEach(function (grp) {
        html += '<div class="palette__group kicker">' + esc(grp.label) + '</div>';
        grp.items.forEach(function (it) {
          var thumb;
          if (it.kind === 'player') thumb = '<span class="hs"><img src="' + esc(it.image) + '" alt="" loading="lazy" onerror="this.remove()"></span>';
          else if (it.image) thumb = '<span class="palette__thumb"><img src="' + esc(it.image) + '" alt=""></span>';
          else thumb = '<span class="palette__thumb">' + (icons[it.kind] || icons.metric) + '</span>';
          html += '<a class="palette__item" href="' + esc(it.url) + '">' + thumb +
            '<span><div class="palette__label">' + esc(it.label) + '</div>' +
            (it.sub ? '<div class="palette__sub">' + esc(it.sub) + '</div>' : '') + '</span>' +
            '<span class="palette__go">↵</span></a>';
        });
      });
      results.innerHTML = html || '<div class="palette__hint">' + T('No matches.', '没有结果。') + '</div>';
      items = $$('.palette__item', results);
      setActive(items.length ? 0 : -1);
    }
    function search(q) {
      q = q.trim();
      if (!q) { results.innerHTML = initialHTML; items = []; active = -1; bindTry(); return; }
      if (q === lastQ) return;
      lastQ = q;
      clearTimeout(timer);
      timer = setTimeout(function () {
        if (controller) controller.abort();
        controller = window.AbortController ? new AbortController() : null;
        fetch(window.FUNBA.searchUrl + '?q=' + encodeURIComponent(q), { signal: controller && controller.signal, headers: { 'Accept': 'application/json' } })
          .then(function (r) { return r.json(); })
          .then(function (d) { if (d.q === lastQ || d.q === q) render(d); })
          .catch(function () {});
      }, 110);
    }
    function bindTry() {
      $$('[data-palette-try]', results).forEach(function (c) {
        c.addEventListener('click', function () { input.value = c.textContent.trim(); search(input.value); input.focus(); });
      });
    }
    bindTry();
    input.addEventListener('input', function () { search(input.value); });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setActive(Math.min(items.length - 1, active + 1)); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(Math.max(0, active - 1)); }
      else if (e.key === 'Enter') { if (items[active]) { e.preventDefault(); window.location.href = items[active].href; } }
    });
    root.addEventListener('mousemove', function (e) {
      var it = e.target.closest('.palette__item');
      if (it) setActive(items.indexOf(it));
    });
    root.addEventListener('click', function (e) { if (e.target === root) close(); });
    document.addEventListener('keydown', function (e) {
      var tag = (document.activeElement && document.activeElement.tagName) || '';
      var typing = /INPUT|TEXTAREA|SELECT/.test(tag) || (document.activeElement && document.activeElement.isContentEditable);
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); root.classList.contains('is-open') ? close() : open(); }
      else if (e.key === '/' && !typing) { e.preventDefault(); open(); }
      else if (e.key === 'Escape' && root.classList.contains('is-open')) close();
    });
    document.addEventListener('click', function (e) {
      var opener = e.target.closest('[data-palette-open]');
      if (opener) { e.preventDefault(); open(opener.getAttribute('data-palette-q')); }
    });
    FX.openSearch = open;
  }

  /* ── Preview flag ──────────────────────────────────────────────────── */
  function initFlag() {
    var flag = $('[data-uiflag]');
    if (!flag) return;
    var dismissed = false;
    try { dismissed = sessionStorage.getItem('funba_uiflag') === '0'; } catch (_) {}
    if (!dismissed) flag.hidden = false;
    var btn = $('[data-uiflag-close]', flag);
    if (btn) btn.addEventListener('click', function () {
      flag.hidden = true;
      try { sessionStorage.setItem('funba_uiflag', '0'); } catch (_) {}
    });
  }

  /* ── Ticker: start at today's games ────────────────────────────────── */
  function initTicker() {
    var tk = $('[data-ticker]');
    if (!tk) return;
    var days = $$('.ticker__day', tk);
    var today = days.length > 1 ? days[1] : null;
    if (today && tk.scrollWidth > tk.clientWidth) {
      var label = $('.ticker__label', tk);
      tk.scrollLeft = Math.max(0, today.offsetLeft - (label ? label.offsetWidth : 0));
    }
  }

  /* ── Count-up numbers ──────────────────────────────────────────────── */
  function initCountUp() {
    $$('[data-count]').forEach(function (el) {
      var target = parseFloat(el.getAttribute('data-count'));
      if (isNaN(target)) return;
      var digits = (String(el.getAttribute('data-count')).split('.')[1] || '').length;
      FX.onVisible(el, function () {
        var start = performance.now(), dur = 900;
        function step(now) {
          var p = Math.min(1, (now - start) / dur);
          var eased = 1 - Math.pow(1 - p, 3);
          el.textContent = (target * eased).toFixed(digits);
          if (p < 1) requestAnimationFrame(step); else el.textContent = target.toFixed(digits);
        }
        requestAnimationFrame(step);
      });
    });
  }

  /* ════════════════════════════════════════════════════════════════════
     Charts
     ════════════════════════════════════════════════════════════════════ */

  function fmtClock(t, periods) {
    // t = minutes elapsed → "Q3 4:32"
    var p, rem;
    if (t <= 48) { p = Math.min(4, Math.floor(t / 12) + 1); if (t >= 48) p = 4; rem = p * 12 - t; }
    else { p = 4 + Math.min(Math.ceil((t - 48) / 5), periods - 4 || 1); rem = 48 + (p - 4) * 5 - t; if (p === 4) p = 5; }
    rem = Math.max(0, rem);
    var m = Math.floor(rem), s = Math.round((rem - m) * 60);
    if (s === 60) { m += 1; s = 0; }
    var label = p <= 4 ? 'Q' + p : 'OT' + (p - 4);
    return label + ' ' + m + ':' + ('0' + s).slice(-2);
  }
  FX.fmtClock = fmtClock;

  /**
   * Game flow chart — margin over time, team colored above/below the axis.
   * opts: {progression:[{t,road,home,scorer,desc}], roadColor, homeColor, roadAbbr, homeAbbr, height}
   */
  FX.flowChart = function (el, opts) {
    var prog = opts.progression || [];
    if (prog.length < 2) { el.innerHTML = '<div class="empty">' + T('No play-by-play yet.', '暂无逐回合数据。') + '</div>'; return; }
    var W = el.clientWidth || 900, H = opts.height || 340;
    var padL = 40, padR = 64, padT = 22, padB = 30;
    var last = prog[prog.length - 1];
    var endT = Math.max(48, Math.ceil(last.t));
    var periods = endT > 48 ? 4 + Math.ceil((endT - 48) / 5) : 4;
    if (periods > 4) endT = 48 + (periods - 4) * 5;
    var maxAbs = 5;
    prog.forEach(function (p) { maxAbs = Math.max(maxAbs, Math.abs(p.home - p.road)); });
    var yMax = Math.ceil((maxAbs + 2) / 5) * 5;
    var x = function (t) { return padL + (t / endT) * (W - padL - padR); };
    var y = function (m) { return padT + (1 - (m + yMax) / (2 * yMax)) * (H - padT - padB); };
    var homeC = uiColor(opts.homeColor), roadC = uiColor(opts.roadColor);

    el.innerHTML = '';
    var svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, width: '100%', height: H, class: 'flow-svg', role: 'img', 'aria-label': T('Score margin over time', '分差走势') }, el);
    var uid = 'f' + Math.random().toString(36).slice(2, 8);
    var defs = svgEl('defs', null, svg);
    var cpTop = svgEl('clipPath', { id: uid + 't' }, defs); svgEl('rect', { x: 0, y: 0, width: W, height: y(0) }, cpTop);
    var cpBot = svgEl('clipPath', { id: uid + 'b' }, defs); svgEl('rect', { x: 0, y: y(0), width: W, height: H }, cpBot);
    var reveal = svgEl('clipPath', { id: uid + 'r' }, defs);
    var revealRect = svgEl('rect', { x: 0, y: 0, width: 0, height: H }, reveal);
    var gradH = svgEl('linearGradient', { id: uid + 'gh', x1: 0, y1: 0, x2: 0, y2: 1 }, defs);
    svgEl('stop', { offset: '0%', 'stop-color': homeC, 'stop-opacity': 0.55 }, gradH);
    svgEl('stop', { offset: '100%', 'stop-color': homeC, 'stop-opacity': 0.08 }, gradH);
    var gradR = svgEl('linearGradient', { id: uid + 'gr', x1: 0, y1: 1, x2: 0, y2: 0 }, defs);
    svgEl('stop', { offset: '0%', 'stop-color': roadC, 'stop-opacity': 0.55 }, gradR);
    svgEl('stop', { offset: '100%', 'stop-color': roadC, 'stop-opacity': 0.08 }, gradR);

    // grid
    var grid = svgEl('g', { class: 'flow-grid' }, svg);
    for (var m = -yMax; m <= yMax; m += (yMax > 20 ? 10 : 5)) {
      svgEl('line', { x1: padL, x2: W - padR, y1: y(m), y2: y(m), stroke: 'var(--rule)', 'stroke-width': m === 0 ? 0 : 1 }, grid);
      if (m !== 0) {
        var lbl = svgEl('text', { x: padL - 8, y: y(m) + 3.5, 'text-anchor': 'end', class: 'flow-axis' }, grid);
        lbl.textContent = Math.abs(m);
      }
    }
    var qEnds = [];
    for (var q = 1; q <= periods; q++) qEnds.push(q <= 4 ? q * 12 : 48 + (q - 4) * 5);
    var prevEnd = 0;
    qEnds.forEach(function (qe, i) {
      if (i % 2 === 1) svgEl('rect', { x: x(prevEnd), y: padT, width: x(qe) - x(prevEnd), height: H - padT - padB, fill: 'var(--hover)' }, grid);
      var lab = svgEl('text', { x: (x(prevEnd) + x(qe)) / 2, y: H - 10, 'text-anchor': 'middle', class: 'flow-axis' }, grid);
      lab.textContent = i < 4 ? 'Q' + (i + 1) : 'OT' + (i - 3);
      prevEnd = qe;
    });

    // step path
    var d = 'M' + x(0) + ',' + y(0);
    var prevM = 0;
    prog.forEach(function (p, i) {
      if (i === 0) return;
      var mm = p.home - p.road;
      d += 'L' + x(p.t).toFixed(1) + ',' + y(prevM).toFixed(1) + 'L' + x(p.t).toFixed(1) + ',' + y(mm).toFixed(1);
      prevM = mm;
    });
    d += 'L' + x(endT).toFixed(1) + ',' + y(prevM).toFixed(1);
    var area = d + 'L' + x(endT) + ',' + y(0) + 'Z';
    var g = svgEl('g', { 'clip-path': 'url(#' + uid + 'r)' }, svg);
    svgEl('path', { d: area, fill: 'url(#' + uid + 'gh)', 'clip-path': 'url(#' + uid + 't)' }, g);
    svgEl('path', { d: area, fill: 'url(#' + uid + 'gr)', 'clip-path': 'url(#' + uid + 'b)' }, g);
    svgEl('path', { d: d, fill: 'none', stroke: homeC, 'stroke-width': 2, 'clip-path': 'url(#' + uid + 't)', 'stroke-linejoin': 'round' }, g);
    svgEl('path', { d: d, fill: 'none', stroke: roadC, 'stroke-width': 2, 'clip-path': 'url(#' + uid + 'b)', 'stroke-linejoin': 'round' }, g);
    svgEl('line', { x1: padL, x2: W - padR, y1: y(0), y2: y(0), stroke: 'var(--ink)', 'stroke-width': 1 }, svg);

    // team labels
    var lt = svgEl('text', { x: padL + 6, y: padT + 12, class: 'flow-team', fill: homeC }, svg); lt.textContent = '▲ ' + opts.homeAbbr;
    var lb = svgEl('text', { x: padL + 6, y: H - padB - 8, class: 'flow-team', fill: roadC }, svg); lb.textContent = '▼ ' + opts.roadAbbr;

    // largest leads markers
    var bestH = null, bestR = null;
    prog.forEach(function (p) {
      var mm = p.home - p.road;
      if (mm > 0 && (!bestH || mm > bestH.m)) bestH = { t: p.t, m: mm };
      if (mm < 0 && (!bestR || -mm > -bestR.m)) bestR = { t: p.t, m: mm };
    });
    [bestH, bestR].forEach(function (b, i) {
      if (!b) return;
      var c = i === 0 ? homeC : roadC;
      var gg = svgEl('g', { class: 'flow-mark' }, g);
      svgEl('circle', { cx: x(b.t), cy: y(b.m), r: 4, fill: 'var(--card)', stroke: c, 'stroke-width': 2 }, gg);
      var tt = svgEl('text', { x: x(b.t), y: y(b.m) + (i === 0 ? -10 : 18), 'text-anchor': 'middle', class: 'flow-note', fill: c }, gg);
      tt.textContent = '+' + Math.abs(b.m);
    });

    // final badge
    var fm = last.home - last.road;
    var fy = y(fm);
    var badge = svgEl('g', { transform: 'translate(' + (W - padR + 8) + ',' + fy + ')' }, svg);
    var bc = fm > 0 ? homeC : fm < 0 ? roadC : 'var(--ink-3)';
    svgEl('rect', { x: 0, y: -12, width: 54, height: 24, rx: 12, fill: bc }, badge);
    var bt = svgEl('text', { x: 27, y: 4.5, 'text-anchor': 'middle', class: 'flow-badge' }, badge);
    bt.textContent = fm === 0 ? T('TIE', '平') : (fm > 0 ? opts.homeAbbr : opts.roadAbbr) + ' +' + Math.abs(fm);

    // hover layer
    var cross = svgEl('line', { y1: padT, y2: H - padB, stroke: 'var(--ink)', 'stroke-width': 1, 'stroke-dasharray': '2 3', opacity: 0 }, svg);
    var dot = svgEl('circle', { r: 4.5, fill: 'var(--ink)', stroke: 'var(--card)', 'stroke-width': 2, opacity: 0 }, svg);
    var hit = svgEl('rect', { x: padL, y: 0, width: W - padL - padR, height: H, fill: 'transparent' }, svg);
    function at(t) {
      var lo = 0, hi = prog.length - 1;
      while (lo < hi) { var mid = (lo + hi + 1) >> 1; if (prog[mid].t <= t) lo = mid; else hi = mid - 1; }
      return prog[lo];
    }
    function move(clientX, clientY) {
      var r = svg.getBoundingClientRect();
      var sx = (clientX - r.left) * (W / r.width);
      var t = Math.max(0, Math.min(endT, ((sx - padL) / (W - padL - padR)) * endT));
      var p = at(t);
      var mm = p.home - p.road;
      cross.setAttribute('x1', x(t)); cross.setAttribute('x2', x(t)); cross.setAttribute('opacity', 0.6);
      dot.setAttribute('cx', x(t)); dot.setAttribute('cy', y(mm)); dot.setAttribute('opacity', 1);
      dot.setAttribute('fill', mm > 0 ? homeC : mm < 0 ? roadC : 'var(--ink)');
      var lead = mm === 0 ? T('Tied', '平局') : (mm > 0 ? opts.homeAbbr : opts.roadAbbr) + ' +' + Math.abs(mm);
      var html = '<div class="kicker">' + fmtClock(t, periods) + '</div>' +
        '<div style="font-family:var(--f-mono);font-size:14px;margin:3px 0;"><b>' + esc(opts.roadAbbr) + ' ' + p.road + '</b> – <b>' + p.home + ' ' + esc(opts.homeAbbr) + '</b> <span style="opacity:.6">· ' + lead + '</span></div>' +
        (p.scorer ? '<div style="opacity:.8">' + esc(p.scorer) + (p.desc ? ' — ' + esc(p.desc) : '') + '</div>' : '');
      FX.tip.show(html, clientX, clientY - 10);
    }
    hit.addEventListener('mousemove', function (e) { move(e.clientX, e.clientY); });
    hit.addEventListener('touchmove', function (e) { var tt = e.touches[0]; move(tt.clientX, tt.clientY); }, { passive: true });
    hit.addEventListener('mouseleave', function () { cross.setAttribute('opacity', 0); dot.setAttribute('opacity', 0); FX.tip.hide(); });

    // draw-in animation
    FX.onVisible(el, function () {
      var start = performance.now(), dur = 1400;
      (function step(now) {
        var pr = Math.min(1, (now - start) / dur);
        var e = 1 - Math.pow(1 - pr, 3);
        revealRect.setAttribute('width', (W * e).toFixed(1));
        if (pr < 1) requestAnimationFrame(step);
      })(start);
    });
  };

  /** Mini margin sparkline (home-positive series). */
  FX.sparkline = function (el) {
    var series;
    try { series = JSON.parse(el.getAttribute('data-spark') || '[]'); } catch (_) { series = []; }
    if (!series.length) return;
    var W = 200, H = 44;
    var maxAbs = Math.max(6, Math.max.apply(null, series.map(Math.abs)));
    var homeC = uiColor(el.getAttribute('data-home')), roadC = uiColor(el.getAttribute('data-road'));
    var x = function (i) { return (i / (series.length - 1)) * W; };
    var y = function (m) { return H / 2 - (m / maxAbs) * (H / 2 - 2); };
    var d = 'M0,' + y(0);
    series.forEach(function (m, i) { d += 'L' + x(i).toFixed(1) + ',' + y(m).toFixed(1); });
    var area = d + 'L' + W + ',' + y(0) + 'Z';
    el.innerHTML = '';
    var svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, preserveAspectRatio: 'none', width: '100%', height: H }, el);
    var uid = 's' + Math.random().toString(36).slice(2, 8);
    var defs = svgEl('defs', null, svg);
    var a = svgEl('clipPath', { id: uid + 'a' }, defs); svgEl('rect', { x: 0, y: 0, width: W, height: H / 2 }, a);
    var b = svgEl('clipPath', { id: uid + 'b' }, defs); svgEl('rect', { x: 0, y: H / 2, width: W, height: H / 2 }, b);
    svgEl('path', { d: area, fill: homeC, opacity: 0.22, 'clip-path': 'url(#' + uid + 'a)' }, svg);
    svgEl('path', { d: area, fill: roadC, opacity: 0.22, 'clip-path': 'url(#' + uid + 'b)' }, svg);
    svgEl('path', { d: d, fill: 'none', stroke: homeC, 'stroke-width': 1.5, 'clip-path': 'url(#' + uid + 'a)', 'vector-effect': 'non-scaling-stroke' }, svg);
    svgEl('path', { d: d, fill: 'none', stroke: roadC, 'stroke-width': 1.5, 'clip-path': 'url(#' + uid + 'b)', 'vector-effect': 'non-scaling-stroke' }, svg);
    svgEl('line', { x1: 0, x2: W, y1: H / 2, y2: H / 2, stroke: 'var(--rule-2)', 'stroke-width': 1, 'vector-effect': 'non-scaling-stroke' }, svg);
  };

  /** Draw an NBA half court into an <svg> group (coords: x -250..250, y -47.5..422.5). */
  FX.drawCourt = function (g, opts) {
    opts = opts || {};
    var line = { fill: 'none', stroke: 'var(--court-line, var(--rule-2))', 'stroke-width': 1.6 };
    function L(attrs) { var o = {}; for (var k in line) o[k] = line[k]; for (var j in attrs) o[j] = attrs[j]; return o; }
    svgEl('rect', L({ x: -250, y: -47.5, width: 500, height: 470 }), g);
    svgEl('rect', L({ x: -80, y: -47.5, width: 160, height: 190 }), g);
    svgEl('rect', L({ x: -60, y: -47.5, width: 120, height: 190 }), g);
    svgEl('path', L({ d: 'M -60 142.5 A 60 60 0 0 0 60 142.5' }), g);
    svgEl('path', L({ d: 'M -60 142.5 A 60 60 0 0 1 60 142.5', 'stroke-dasharray': '5 6' }), g);
    svgEl('path', L({ d: 'M -40 0 A 40 40 0 0 0 40 0' }), g);
    svgEl('line', L({ x1: -30, x2: 30, y1: -7.5, y2: -7.5, 'stroke-width': 2.4 }), g);
    svgEl('circle', L({ cx: 0, cy: 0, r: 7.5 }), g);
    // three point line: corners + arc
    svgEl('path', L({ d: 'M -220 -47.5 L -220 92.5 A 237.5 237.5 0 0 0 220 92.5 L 220 -47.5' }), g);
    svgEl('path', L({ d: 'M -60 422.5 A 60 60 0 0 1 60 422.5' }), g);
    svgEl('path', L({ d: 'M -20 422.5 A 20 20 0 0 1 20 422.5' }), g);
  };

  /**
   * Shot chart. shots: [{x,y,made,player_name,shot_type,shot_distance,period_label,clock}]
   * opts: {color, filter: fn}
   */
  FX.shotChart = function (el, shots, opts) {
    opts = opts || {};
    el.innerHTML = '';
    var svg = svgEl('svg', { viewBox: '-252 -50 504 330', width: '100%', class: 'court-svg', role: 'img', 'aria-label': T('Shot chart', '投篮分布') }, el);
    var court = svgEl('g', null, svg);
    FX.drawCourt(court);
    var color = uiColor(opts.color || '#FF4F12');
    var layer = svgEl('g', null, svg);
    var list = shots.filter(function (s) { return s.y <= 280 && (!opts.filter || opts.filter(s)); });
    list.sort(function (a, b) { return (a.made ? 1 : 0) - (b.made ? 1 : 0); });
    list.forEach(function (s, i) {
      var node;
      if (s.made) {
        node = svgEl('circle', { cx: s.x, cy: s.y, r: 5.2, fill: color, 'fill-opacity': 0.9, stroke: 'var(--card)', 'stroke-width': 1.2, class: 'shot shot--made' }, layer);
      } else {
        node = svgEl('path', { d: 'M' + (s.x - 3.6) + ' ' + (s.y - 3.6) + 'l7.2 7.2M' + (s.x + 3.6) + ' ' + (s.y - 3.6) + 'l-7.2 7.2', stroke: 'var(--ink-3)', 'stroke-width': 1.6, 'stroke-linecap': 'round', class: 'shot shot--miss' }, layer);
      }
      node.style.animationDelay = Math.min(900, i * 4) + 'ms';
      node.addEventListener('mouseenter', function (e) {
        FX.tip.show('<div class="kicker">' + esc(s.period_label || '') + ' ' + esc(s.clock || '') + '</div><b>' + esc(s.player_name || '') + '</b><div>' +
          (s.made ? T('Made', '命中') : T('Missed', '未中')) + ' · ' + esc(s.shot_type || '') + (s.shot_distance !== '-' && s.shot_distance != null ? ' · ' + esc(s.shot_distance) + 'ft' : '') + '</div>', e.clientX, e.clientY);
      });
      node.addEventListener('mouseleave', FX.tip.hide);
    });
    return { count: list.length, made: list.filter(function (s) { return s.made; }).length };
  };

  /**
   * Column chart. data: [{v, label, color, tip, href}]
   * opts: {height, zero (bool, diverging), max}
   */
  FX.columns = function (el, data, opts) {
    opts = opts || {};
    el.innerHTML = '';
    if (!data.length) return;
    var W = el.clientWidth || 800, H = opts.height || 160;
    var pad = { l: opts.axis ? 28 : 0, r: 0, t: 8, b: opts.labels ? 20 : 4 };
    var vals = data.map(function (d) { return d.v || 0; });
    var max = opts.max != null ? opts.max : Math.max.apply(null, vals.concat([0.0001]));
    var min = opts.diverging ? Math.min.apply(null, vals.concat([0])) : 0;
    if (opts.diverging) { var m = Math.max(Math.abs(max), Math.abs(min)); max = m; min = -m; }
    var svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, width: '100%', height: H }, el);
    var innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
    var bw = innerW / data.length;
    var gap = Math.max(1, Math.min(6, bw * 0.22));
    var y = function (v) { return pad.t + (1 - (v - min) / (max - min || 1)) * innerH; };
    if (opts.refLine != null) {
      svgEl('line', { x1: pad.l, x2: W - pad.r, y1: y(opts.refLine), y2: y(opts.refLine), stroke: 'var(--rule-2)', 'stroke-dasharray': '3 4' }, svg);
    }
    if (opts.axis) {
      [max, opts.diverging ? 0 : max / 2].forEach(function (v) {
        var tx = svgEl('text', { x: pad.l - 6, y: y(v) + 3, 'text-anchor': 'end', class: 'flow-axis' }, svg);
        tx.textContent = opts.fmt ? opts.fmt(v) : Math.round(v);
      });
    }
    var g = svgEl('g', null, svg);
    data.forEach(function (d, i) {
      var v = d.v || 0;
      var x0 = pad.l + i * bw + gap / 2;
      var y0 = y(Math.max(v, opts.diverging ? 0 : min)), y1 = y(Math.min(v, opts.diverging ? 0 : min));
      var h = Math.max(1.5, y1 - y0);
      var r = svgEl('rect', { x: x0, y: y0, width: Math.max(1, bw - gap), height: h, rx: Math.min(2, (bw - gap) / 3), fill: uiColor(d.color) || 'var(--ink)', class: 'col' }, g);
      r.style.transformOrigin = (x0 + (bw - gap) / 2) + 'px ' + y(opts.diverging ? 0 : min) + 'px';
      r.style.animationDelay = Math.min(600, i * 12) + 'ms';
      if (d.tip) {
        r.addEventListener('mouseenter', function (e) { r.setAttribute('opacity', 0.75); FX.tip.show(d.tip, e.clientX, e.clientY); });
        r.addEventListener('mousemove', function (e) { FX.tip.show(d.tip, e.clientX, e.clientY); });
        r.addEventListener('mouseleave', function () { r.setAttribute('opacity', 1); FX.tip.hide(); });
      }
      if (d.href) { r.style.cursor = 'pointer'; r.addEventListener('click', function () { window.location.href = d.href; }); }
      if (opts.labels && d.label && (data.length < 40 || i % Math.ceil(data.length / 20) === 0)) {
        var tx = svgEl('text', { x: x0 + (bw - gap) / 2, y: H - 5, 'text-anchor': 'middle', class: 'flow-axis' }, svg);
        tx.textContent = d.label;
      }
    });
    if (opts.diverging) svgEl('line', { x1: pad.l, x2: W - pad.r, y1: y(0), y2: y(0), stroke: 'var(--ink)', 'stroke-width': 1 }, svg);
  };

  /* ── Boot ──────────────────────────────────────────────────────────── */
  function boot() {
    initReveal();
    initPalette();
    initFlag();
    initTicker();
    initCountUp();
    $$('[data-spark]').forEach(FX.sparkline);
    document.dispatchEvent(new CustomEvent('fx:ready'));
  }
  document.addEventListener('fx:theme', function () { $$('[data-spark]').forEach(FX.sparkline); });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();

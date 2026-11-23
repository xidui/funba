/* FUNBA Next — player page charts */
(function () {
  'use strict';
  function boot() {
    var FX = window.FX;
    var T = FX.t, esc = FX.esc, $ = FX.$, $$ = FX.$$;
    var dataEl = document.getElementById('pl-data');
    if (!dataEl) return;
    var D = {};
    try { D = JSON.parse(dataEl.textContent); } catch (_) { return; }
    var tc = D.color || '#6B6862';
    function isDark() { return document.documentElement.getAttribute('data-theme') === 'dark'; }
    function lossColor() { return isDark() ? '#4A4640' : '#C9C1B1'; }

    /* ── colour scale: FG% delta vs expected ─────────────────────────── */
    function hexToRgb(h) { return [parseInt(h.substr(1, 2), 16), parseInt(h.substr(3, 2), 16), parseInt(h.substr(5, 2), 16)]; }
    function mix(a, b, t) {
      var A = hexToRgb(a), B = hexToRgb(b);
      return 'rgb(' + [0, 1, 2].map(function (i) { return Math.round(A[i] + (B[i] - A[i]) * t); }).join(',') + ')';
    }
    function heat(delta) {
      var cold = '#2F6DB5', hot = '#FF4F12', mid = isDark() ? '#5B564D' : '#D7CEBD';
      var t = Math.max(-1, Math.min(1, delta / 0.11));
      return t < 0 ? mix(mid, cold, -t) : mix(mid, hot, t);
    }
    FX.heat = heat;
    var ZONE_EXP = {
      restricted_area: 0.64, paint_non_ra: 0.43,
      left_mid_range: 0.41, center_mid_range: 0.42, right_mid_range: 0.41,
      left_corner_3: 0.385, right_corner_3: 0.385,
      left_above_break_3: 0.355, center_above_break_3: 0.355, right_above_break_3: 0.355
    };
    function expectedAt(x, y) {
      var d = Math.sqrt(x * x + y * y) / 10;
      var corner = Math.abs(x) >= 220 && y <= 92.5;
      if (corner) return 0.385;
      if (d >= 23.75) return 0.355;
      if (d < 4) return 0.64;
      if (d < 9) return 0.45;
      if (d < 16) return 0.42;
      return 0.405;
    }

    function capWidth(el, n) { el.style.maxWidth = n < 8 ? Math.max(260, n * 96) + 'px' : ''; }

    /* ── reveal buttons ──────────────────────────────────────────────── */
    $$('[data-reveal]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        $$(btn.getAttribute('data-reveal')).forEach(function (el) { el.hidden = false; el.removeAttribute('data-rk-more'); el.removeAttribute('data-gl-more'); });
        btn.remove();
      });
    });

    /* ── game log ────────────────────────────────────────────────────── */
    var glStat = 'pts';
    function minutes(m) {
      if (m == null) return 0;
      var p = String(m).split(':');
      return p.length === 2 ? (+p[0] + (+p[1]) / 60) : (+m || 0);
    }
    function glValue(g, k) { return k === 'min' ? minutes(g.min) : (+g[k] || 0); }
    function drawGameLog() {
      var el = document.getElementById('gl-chart');
      if (!el || !D.gameLog || !D.gameLog.length) return;
      capWidth(el, D.gameLog.length);
      var played = D.gameLog.filter(function (g) { return minutes(g.min) > 0; });
      var vals = played.map(function (g) { return glValue(g, glStat); });
      var avg = vals.length ? vals.reduce(function (a, b) { return a + b; }, 0) / vals.length : 0;
      var best = null;
      D.gameLog.forEach(function (g) { if (!best || glValue(g, glStat) > glValue(best, glStat)) best = g; });
      var avgEl = $('[data-gl-avg]'), bestEl = $('[data-gl-best]');
      if (avgEl) avgEl.textContent = avg.toFixed(1);
      if (bestEl && best) bestEl.innerHTML = (glStat === 'min' ? Math.round(glValue(best, 'min')) : glValue(best, glStat)) + '<small class="muted"> ' + esc((best.home ? 'vs ' : '@ ') + best.opp) + '</small>';
      FX.columns(el, D.gameLog.map(function (g) {
        var v = glValue(g, glStat);
        return {
          v: v,
          color: g.result === 'W' ? tc : lossColor(),
          href: g.url,
          tip: '<div class="kicker">' + esc(g.date) + ' · ' + (g.home ? 'vs ' : '@ ') + esc(g.opp) + '</div>' +
               '<div style="margin:3px 0;"><b>' + (g.result === 'W' ? T('W', '胜') : g.result === 'L' ? T('L', '负') : '–') + ' ' + esc(g.score) + '</b></div>' +
               '<div class="mono" style="font-size:12px">PTS ' + g.pts + ' · REB ' + g.reb + ' · AST ' + g.ast + ' · ' + esc(g.min || '0:00') + '</div>' +
               (minutes(g.min) === 0 ? '<div style="opacity:.7">' + esc(g.status || T('Did not play', '未出场')) + '</div>' : '')
        };
      }), { height: 210, refLine: avg, axis: true, fmt: function (v) { return Math.round(v); } });
    }
    var glSeg = $('[data-gl-stat]');
    if (glSeg) glSeg.addEventListener('click', function (e) {
      var b = e.target.closest('[data-k]'); if (!b) return;
      $$('[data-k]', glSeg).forEach(function (x) { x.classList.toggle('is-on', x === b); });
      glStat = b.getAttribute('data-k');
      drawGameLog();
    });

    /* ── career arc ──────────────────────────────────────────────────── */
    var arcStat = 'ppg';
    function drawArc() {
      var el = document.getElementById('arc-chart');
      if (!el || !D.career || !D.career.length) return;
      capWidth(el, D.career.length);
      var isPct = /_pct$/.test(arcStat);
      FX.columns(el, D.career.map(function (s) {
        var raw = s[arcStat];
        var v = raw == null ? 0 : (isPct ? raw * 100 : raw);
        var yr = s.year ? ("'" + String(s.year % 100).padStart(2, '0')) : '';
        return {
          v: v,
          label: yr,
          color: s.color || tc,
          tip: '<div class="kicker">' + (s.year ? s.year + '–' + String((s.year + 1) % 100).padStart(2, '0') : '') + ' · ' + esc((s.teams || []).join(' / ')) + '</div>' +
               '<div style="font-family:var(--f-mono);font-size:15px;margin-top:3px;"><b>' + (raw == null ? '—' : v.toFixed(1) + (isPct ? '%' : '')) + '</b> ' + arcStat.replace('_pct', '%').toUpperCase().replace('%', isPct ? '%' : '') + '</div>' +
               '<div style="opacity:.75">' + (s.gp != null ? s.gp + ' ' + T('games', '场') : '') + '</div>'
        };
      }), { height: 240, axis: true, labels: true, fmt: function (v) { return isPct ? Math.round(v) + '%' : Math.round(v); } });
    }
    var arcSeg = $('[data-arc-stat]');
    if (arcSeg) arcSeg.addEventListener('click', function (e) {
      var b = e.target.closest('[data-k]'); if (!b) return;
      $$('[data-k]', arcSeg).forEach(function (x) { x.classList.toggle('is-on', x === b); });
      arcStat = b.getAttribute('data-k');
      drawArc();
    });

    /* ── shot charts ─────────────────────────────────────────────────── */
    var SVGNS = 'http://www.w3.org/2000/svg';
    function courtSvg(el) {
      el.innerHTML = '';
      var svg = FX.svgEl('svg', { viewBox: '-252 -52 504 384', width: '100%', class: 'court-svg', role: 'img' }, el);
      var defs = FX.svgEl('defs', null, svg);
      var cp = FX.svgEl('clipPath', { id: 'plclip' + el.id }, defs);
      FX.svgEl('rect', { x: -250, y: -47.5, width: 500, height: 378 }, cp);
      return svg;
    }
    function drawHex() {
      var el = document.getElementById('hex-court');
      if (!el || !D.shots || !D.shots.length) return;
      var svg = courtSvg(el);
      var flat = D.shots, n = flat.length / 3;
      var r = n > 3000 ? 10 : n > 800 ? 12.5 : n > 250 ? 15 : 19;
      var dx = r * 2 * Math.sin(Math.PI / 3), dy = r * 1.5;
      var bins = {};
      for (var i = 0; i < flat.length; i += 3) {
        var x = flat[i], y = flat[i + 1], m = flat[i + 2];
        if (y > 330) continue;
        var py = y / dy, pj = Math.round(py), px = x / dx - (pj & 1) / 2, pi = Math.round(px), py1 = py - pj;
        if (Math.abs(py1) * 3 > 1) {
          var px1 = px - pi, pi2 = pi + (px < pi ? -1 : 1) / 2, pj2 = pj + (py < pj ? -1 : 1), px2 = px - pi2, py2 = py - pj2;
          if (px1 * px1 + py1 * py1 > px2 * px2 + py2 * py2) { pi = pi2 + (pj & 1 ? 1 : -1) / 2; pj = pj2; }
        }
        var key = pi + ',' + pj;
        var b = bins[key] || (bins[key] = { x: (pi + (pj & 1) / 2) * dx, y: pj * dy, a: 0, m: 0 });
        b.a++; b.m += m;
      }
      var list = Object.keys(bins).map(function (k) { return bins[k]; });
      var maxA = list.reduce(function (mx, b) { return Math.max(mx, b.a); }, 1);
      var cap = Math.max(4, maxA * 0.35);               // rim bins dominate; cap so others stay visible
      var minA = Math.max(1, Math.ceil(n / 2500));
      var layer = FX.svgEl('g', { 'clip-path': 'url(#plclip' + el.id + ')' }, svg);
      list.sort(function (a, b) { return a.a - b.a; });
      list.forEach(function (b, idx) {
        if (b.a < minA) return;
        var exp = expectedAt(b.x, b.y);
        var fg = (b.m + exp * 3) / (b.a + 3);
        var s = 0.28 + 0.72 * Math.sqrt(Math.min(1, b.a / cap));
        var rr = r * s * 0.98, d = '';
        for (var k = 0; k < 6; k++) {
          var ang = k * Math.PI / 3;
          d += (k ? 'L' : 'M') + (b.x + Math.sin(ang) * rr).toFixed(1) + ',' + (b.y - Math.cos(ang) * rr).toFixed(1);
        }
        var hex = FX.svgEl('path', { d: d + 'Z', fill: heat(fg - exp), class: 'shot pl-hex' }, layer);
        hex.style.animationDelay = Math.min(700, idx * 2) + 'ms';
        hex.addEventListener('mouseenter', function (e) {
          var ft = Math.round(Math.sqrt(b.x * b.x + b.y * b.y) / 10);
          FX.tip.show('<div class="kicker">~' + ft + ' ft</div><b>' + b.m + '/' + b.a + '</b> · ' + (b.m / b.a * 100).toFixed(1) + '% <div style="opacity:.75">' +
            T('Typical', '常规水平') + ' ' + Math.round(exp * 100) + '%</div>', e.clientX, e.clientY);
        });
        hex.addEventListener('mouseleave', FX.tip.hide);
      });
      FX.drawCourt(FX.svgEl('g', { class: 'pl-court-lines' }, svg));
    }
    var zonesDrawn = false;
    function drawZones() {
      var el = document.getElementById('zone-court');
      if (!el || !D.zones || !D.zones.length) return;
      zonesDrawn = true;
      var svg = courtSvg(el);
      var g = FX.svgEl('g', { 'clip-path': 'url(#plclip' + el.id + ')' }, svg);
      D.zones.forEach(function (z) {
        var fg = parseFloat(z.fg_pct);
        var exp = ZONE_EXP[z.zone_key] || 0.4;
        var fill = z.attempts ? heat((isNaN(fg) ? exp : fg) - exp) : (isDark() ? '#1E1E1B' : '#ECE7DC');
        var p = FX.svgEl('path', { d: z.path_d, fill: fill, stroke: 'var(--card)', 'stroke-width': 2.5, class: 'pl-zone' }, g);
        p.addEventListener('mouseenter', function (e) {
          FX.tip.show('<div class="kicker">' + esc(z.zone_label) + '</div><b>' + z.made + '/' + z.attempts + '</b> · ' + (isNaN(fg) ? '—' : (fg * 100).toFixed(1) + '%') +
            '<div style="opacity:.75">' + T('Typical', '常规水平') + ' ' + Math.round(exp * 100) + '%</div>', e.clientX, e.clientY);
        });
        p.addEventListener('mouseleave', FX.tip.hide);
      });
      FX.drawCourt(FX.svgEl('g', { class: 'pl-court-lines' }, svg));
      D.zones.forEach(function (z) {
        if (!z.attempts || z.y > 330) return;
        var fg = parseFloat(z.fg_pct);
        var t1 = FX.svgEl('text', { x: z.x, y: z.y + 2, 'text-anchor': 'middle', class: 'pl-zone-pct' }, svg);
        t1.textContent = isNaN(fg) ? '—' : (fg * 100).toFixed(1) + '%';
        var t2 = FX.svgEl('text', { x: z.x, y: z.y + 17, 'text-anchor': 'middle', class: 'pl-zone-n' }, svg);
        t2.textContent = z.made + '/' + z.attempts;
      });
    }
    function colorZoneList() {
      $$('.pl-zones__fg').forEach(function (el) {
        var fg = parseFloat(el.getAttribute('data-fg'));
        var exp = ZONE_EXP[el.getAttribute('data-key')] || 0.4;
        if (!isNaN(fg)) el.style.setProperty('--heat', heat(fg - exp));
      });
    }
    document.addEventListener('fx:tab', function (e) {
      if (e.detail.scope === 'shotview') {
        if (e.detail.key === 'zones' && !zonesDrawn) drawZones();
        $$('[data-hex-only]').forEach(function (x) { x.style.visibility = e.detail.key === 'hex' ? '' : 'hidden'; });
      }
    });

    /* ── rankings filter ─────────────────────────────────────────────── */
    $$('[data-rk-cats]').forEach(function (bar) {
      bar.addEventListener('click', function (e) {
        var b = e.target.closest('[data-cat]'); if (!b) return;
        $$('[data-cat]', bar).forEach(function (x) { x.classList.toggle('chip--solid', x === b); });
        var cat = b.getAttribute('data-cat');
        var panel = bar.closest('.pl-rk');
        var more = panel.querySelector('.pl-more');
        $$('.pl-rk__row', panel).forEach(function (row, i) {
          if (cat) row.hidden = row.getAttribute('data-c') !== cat;
          else row.hidden = row.hasAttribute('data-rk-more');
        });
        if (more) more.hidden = !!cat;
      });
    });

    /* ── money ───────────────────────────────────────────────────────── */
    function drawMoney() {
      var el = document.getElementById('money-chart');
      if (!el) return;
      var rows = (D.money && D.money.length) ? D.money : (D.moneyFallback || []);
      if (!rows.length) return;
      capWidth(el, rows.length);
      var fc = D.flavorColors || {};
      FX.columns(el, rows.map(function (r) {
        var v = (r.cap_hit_usd || 0) / 1e6;
        return {
          v: v,
          label: "'" + String(r.season % 100).padStart(2, '0'),
          color: r.is_future ? (isDark() ? '#3A3733' : '#D9D2C4') : (fc[r.flavor] || '#8B8880'),
          tip: '<div class="kicker">' + r.season + '–' + String((r.season + 1) % 100).padStart(2, '0') + (r.team_abbr ? ' · ' + esc(r.team_abbr) : '') + (r.age ? ' · ' + T('age', '年龄') + ' ' + r.age : '') + '</div>' +
               '<b style="font-family:var(--f-mono);font-size:15px">$' + v.toFixed(1) + 'M</b>' + (r.flavor ? ' <span style="opacity:.7">' + esc(String(r.flavor).replace('-', ' ')) + '</span>' : '') +
               (r.is_future ? '<div style="opacity:.7">' + T('Future season', '未来赛季') + '</div>' : '')
        };
      }), { height: 200, axis: true, labels: true, fmt: function (v) { return v >= 10 ? Math.round(v) + 'M' : v >= 1 ? v.toFixed(1) + 'M' : Math.round(v * 1000) + 'K'; } });
    }

    /* ── section nav highlight ───────────────────────────────────────── */
    var links = $$('.pl-nav a[href^="#"]');
    if (links.length && 'IntersectionObserver' in window) {
      var map = {};
      links.forEach(function (a) { var s = document.querySelector(a.getAttribute('href')); if (s) map[s.id] = a; });
      var io = new IntersectionObserver(function (entries) {
        entries.forEach(function (en) {
          if (en.isIntersecting) { links.forEach(function (a) { a.classList.remove('is-on'); }); if (map[en.target.id]) map[en.target.id].classList.add('is-on'); }
        });
      }, { rootMargin: '-45% 0px -50% 0px' });
      Object.keys(map).forEach(function (id) { io.observe(document.getElementById(id)); });
    }

    function drawAll() {
      drawGameLog(); drawArc(); drawMoney(); colorZoneList();
      if (document.getElementById('hex-court')) FX.onVisible(document.getElementById('hex-court'), drawHex);
      if (zonesDrawn) drawZones();
    }
    drawAll();
    document.addEventListener('fx:theme', function () { drawGameLog(); drawArc(); drawMoney(); colorZoneList(); drawHex(); if (zonesDrawn) drawZones(); });
    var rt;
    window.addEventListener('resize', function () { clearTimeout(rt); rt = setTimeout(function () { drawGameLog(); drawArc(); drawMoney(); }, 160); });
  }
  if (window.FX && document.readyState !== 'loading') boot();
  else document.addEventListener('fx:ready', boot);
})();

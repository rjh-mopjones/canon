// ═══ CANON HEADER INJECTION ═══
// Injects the Canon header bar into the mdBook page and syncs theme
// with canon-site's localStorage key for seamless navigation.

(function() {
  'use strict';

  // Create the Canon header
  var header = document.createElement('header');
  header.className = 'canon-header';
  header.innerHTML = [
    '<div class="canon-hdr-left">',
    '  <a class="canon-logo" href="/">Canon</a>',
    '  <a class="canon-nav-link" href="/">Home</a>',
    '  <a class="canon-nav-link" href="/#features">Features</a>',
    '  <a class="canon-nav-link active" href="/docs/">Docs</a>',
    '  <a class="canon-nav-link" href="/demo">Demo</a>',
    '</div>',
    '<div class="canon-hdr-r">',
    '  <a class="canon-gh-link" href="https://github.com/rjh-mopjones/canon" target="_blank" rel="noopener">',
    '    <svg viewBox="0 0 16 16"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>',
    '    <span>GitHub</span>',
    '  </a>',
    '  <div class="canon-theme-btn" id="canonThemeToggle">',
    '    <div class="canon-trk"><div class="canon-thmb"></div></div>',
    '    <span class="canon-theme-label" id="canonThemeLabel">Dark</span>',
    '  </div>',
    '</div>'
  ].join('\n');

  document.body.insertBefore(header, document.body.firstChild);

  // ═══ THEME SYNCING ═══
  // canon-site uses body.dark class and 'canon-theme' localStorage key.
  // mdBook uses its own theme system. We bridge between them.

  var STORAGE_KEY = 'canon-theme';

  function getCanonTheme() {
    return localStorage.getItem(STORAGE_KEY) || 'light';
  }

  function setCanonTheme(theme) {
    localStorage.setItem(STORAGE_KEY, theme);
  }

  function applyTheme(theme) {
    var mdBookTheme = theme === 'dark' ? 'coal' : 'light';

    // Use mdBook's built-in theme setter if available
    if (typeof window.themeManager !== 'undefined' && window.themeManager.set) {
      window.themeManager.set(mdBookTheme);
    } else {
      // Fallback: set class directly
      document.documentElement.className = mdBookTheme;
      // Also set on html for mdBook
      var html = document.querySelector('html');
      if (html) {
        html.className = html.className.replace(/\b(light|rust|coal|navy|ayu)\b/g, '') + ' ' + mdBookTheme;
      }
    }

    // Update the toggle label
    var label = document.getElementById('canonThemeLabel');
    if (label) {
      label.textContent = theme === 'dark' ? 'Light' : 'Dark';
    }

    setCanonTheme(theme);
  }

  // Apply saved theme on load
  var savedTheme = getCanonTheme();
  applyTheme(savedTheme);

  // Toggle handler
  var toggleBtn = document.getElementById('canonThemeToggle');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      var current = getCanonTheme();
      var next = current === 'dark' ? 'light' : 'dark';
      applyTheme(next);
    });
  }

  // Listen for storage changes from other tabs (canon-site <-> docs sync)
  window.addEventListener('storage', function(e) {
    if (e.key === STORAGE_KEY && e.newValue) {
      applyTheme(e.newValue);
    }
  });

  // Also sync with mdBook's own theme changes
  var observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(m) {
      if (m.attributeName === 'class') {
        var html = document.documentElement;
        if (html.classList.contains('coal') || html.classList.contains('navy') || html.classList.contains('ayu')) {
          setCanonTheme('dark');
          var label = document.getElementById('canonThemeLabel');
          if (label) label.textContent = 'Light';
        } else if (html.classList.contains('light') || html.classList.contains('rust')) {
          setCanonTheme('light');
          var label = document.getElementById('canonThemeLabel');
          if (label) label.textContent = 'Dark';
        }
      }
    });
  });
  observer.observe(document.documentElement, { attributes: true });
})();

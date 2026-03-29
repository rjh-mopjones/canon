// ═══ THEME TOGGLE ═══
function toggleTheme() {
  document.body.classList.toggle('dark');
  document.getElementById('themeLabel').textContent =
    document.body.classList.contains('dark') ? 'Light' : 'Dark';
}

// ═══ PIPELINE ANIMATION ═══
(function() {
  var row1 = document.getElementById('pipeRow1');
  var row2 = document.getElementById('pipeRow2');
  var bend = document.getElementById('pipeBend');
  var fanout = document.getElementById('pipeFanout');

  var r1Nodes = row1.querySelectorAll('.pipe-node');
  var r1Arrows = row1.querySelectorAll('.pipe-arrow');
  var r2Nodes = row2.querySelectorAll(':scope > .pipe-node');
  var r2Arrows = row2.querySelectorAll(':scope > .pipe-arrow');
  var fanRows = fanout.querySelectorAll('.pipe-fanout-row');

  var total = r1Nodes.length + 1 + r2Nodes.length + 1 + 1;
  var idx = 0;

  function reset() {
    r1Nodes.forEach(function(n) { n.classList.remove('active'); });
    r1Arrows.forEach(function(a) { a.classList.remove('active'); });
    r2Nodes.forEach(function(n) { n.classList.remove('active'); });
    r2Arrows.forEach(function(a) { a.classList.remove('active'); });
    bend.classList.remove('active');
    fanout.classList.remove('active');
    fanRows.forEach(function(r) {
      r.classList.remove('active');
      r.querySelector('.pipe-node').classList.remove('active');
    });
  }

  function step() {
    if (idx === 0) reset();

    var r1Len = r1Nodes.length;
    var bendIdx = r1Len;
    var r2Start = bendIdx + 1;
    var r2Len = r2Nodes.length;
    var fanIdx = r2Start + r2Len;

    if (idx < r1Len) {
      r1Nodes[idx].classList.add('active');
      if (idx > 0) r1Arrows[idx - 1].classList.add('active');
    } else if (idx === bendIdx) {
      r1Arrows[r1Len - 1].classList.add('active');
      bend.classList.add('active');
    } else if (idx >= r2Start && idx < fanIdx) {
      var ri = idx - r2Start;
      r2Nodes[ri].classList.add('active');
      if (ri > 0) r2Arrows[ri - 1].classList.add('active');
    } else if (idx === fanIdx) {
      r2Arrows[r2Len - 1].classList.add('active');
      fanout.classList.add('active');
      fanRows.forEach(function(r) {
        r.classList.add('active');
        r.querySelector('.pipe-node').classList.add('active');
      });
    }

    idx++;
    if (idx > total) {
      idx = 0;
      setTimeout(step, 1400);
    } else {
      setTimeout(step, 450);
    }
  }

  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(e) {
      if (e.isIntersecting) {
        step();
        observer.disconnect();
      }
    });
  }, { threshold: 0.3 });
  observer.observe(document.getElementById('pipelineViz'));
})();

// ═══ DEMO SHIP ANIMATION ═══
(function() {
  var ship = document.getElementById('demoShip');
  var positions = [
    { left: '18%', top: '26%' },
    { left: '68%', top: '14%' },
    { left: '76%', top: '68%' },
    { left: '24%', top: '74%' }
  ];
  var pos = 0;

  setInterval(function() {
    pos = (pos + 1) % positions.length;
    ship.style.left = positions[pos].left;
    ship.style.top = positions[pos].top;
  }, 4000);
})();

// ═══ NAV SCROLL SPY ═══
document.querySelectorAll('.nav-link[href^="#"]').forEach(function(link) {
  link.addEventListener('click', function(e) {
    e.preventDefault();
    var id = link.getAttribute('href').slice(1);
    var el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: 'smooth' });
  });
});

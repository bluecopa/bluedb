// Render mermaid diagrams under the shadcn theme.
//
// Material rendered mermaid out of the box; shadcn does not, so we load
// mermaid.js (see `extra_javascript` in mkdocs.yml) and run it against the
// blocks the pymdownx superfence emits as `<pre class="mermaid">…</pre>`.
(function () {
  function render() {
    if (!window.mermaid) return;
    window.mermaid.initialize({ startOnLoad: false, securityLevel: "loose" });
    // Render every superfence block (and any bare `.mermaid` element).
    window.mermaid.run({ querySelector: "pre.mermaid, .mermaid" });
  }
  if (document.readyState !== "loading") {
    render();
  } else {
    document.addEventListener("DOMContentLoaded", render);
  }
})();

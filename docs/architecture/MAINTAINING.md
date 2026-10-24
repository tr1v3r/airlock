# Maintaining the architecture atlas

The authored source is each `candidate.json` linked by `manifest.json`.
Standalone HTML is a generated, checked artifact, not hand-edited documentation.
The generation working directory is `docs/architecture`; `meta.output` is relative
to it. Each diagram keeps its candidate and HTML in a timestamped `.archify` folder.

## Reproduce

The current artifacts were generated with **Archify 3.1.0**, Node 26.11.1 and
Chrome on macOS. Node and Chrome are documentation tooling only, not Airlock
runtime dependencies. Install/provide Archify separately; the Go module does not
download it. From the repository root:

```sh
node docs/architecture/rebuild.mjs /path/to/archify/bin/archify.mjs
```

The script runs `finalize` with repository evidence, showcase checks and the
real-browser gate. It stops on the first failure and creates a new review directory
so previous browser evidence is retained. Chrome must be allowed to start; a
browser runtime failure is not a passed check. Use the same Archify version for
reproduction; a newer renderer may change output bytes and needs a fresh review.

## Change a diagram

1. Trace the actual caller, state mutation and persistence boundary in source.
2. Update the candidate and its `meta.repository.revision` plus source ranges as
   one change. Evidence is validated against committed source bytes, not local edits.
3. Keep the origin identity credential-free. This atlas preserves the SSH origin
   with `link_mode: local-only`; the README links to the same pinned GitHub revision.
4. Run `rebuild.mjs`. Preserve business conditions while repairing layout failures.
5. Inspect changed diagrams in the browser, including complete paths below the fold.
   Keep full-page light-theme PNG previews in `previews/`; these are browser
   screenshots, not canonical SVG/PNG exports. The viewer's Export menu provides
   canonical diagram exports without transient UI.
6. Refresh `verification.json` from the successful finalizer receipts, binding
   every candidate and HTML to its SHA-256. Do not mark a perceptual review passed
   solely because screenshots were captured. Record its actual viewport/theme scope.
7. Update the bilingual guides, overview links and manifest together. Verify local
   links and ensure the main READMEs still agree. Never replace diagrams with
   screenshots alone: JSON and standalone HTML must remain available.

Machine-local receipts and capture sets are ignored because they contain absolute
checkout/tool paths and environment details. `verification.json` is the portable
inventory of checked artifacts; it is not a replacement for rerunning the gates.

## Scope decisions

The atlas contains eight complementary views rather than every supported diagram
type. The source/test entry-point tables cover repository navigation without a
second directory tree. There is no class diagram that merely repeats every API,
no proposed relational schema presented as shipped, and no timing waterfall without
a measured trace. Add a view when it answers a concrete reader question that the
existing views do not answer.

The backend lifecycle intentionally preserves crossings to retain all terminal
paths. Focus a state in the HTML viewer to inspect incoming/outgoing transitions.
This drawing distinguishes `Rejected` (a stored queue rejection) from protocol
errors that do not create that status. The client Lease view is a separate state
space and must never be merged with authoritative backend status.

The pinned implementation snapshot is `beb9c6fcd49f9d270bfb5f951ac0b400b5aeca0d`.
The PR base `045c387eabd1913988884c98a31b43e40f67570a` adds SDK comments and README
routing. This documentation-only change does not alter Go behavior or claim new
backend integration/performance results.

## Viewer attribution

Generated HTML embeds Archify's viewer. Its MIT notice is preserved in
[ARCHIFY-LICENSE.txt](ARCHIFY-LICENSE.txt); this tooling attribution does not change
Airlock's own license.

Generated HTML is marked `linguist-generated` for review. Renderer-emitted trailing
spaces are preserved to keep the validated bytes and hashes intact; whitespace
checking is relaxed only for those generated HTML files. Review authored JSON,
guides and previews alongside the generated result.

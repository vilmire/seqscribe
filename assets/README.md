# Brand assets

Two hand-written SVGs, no build step and no dependencies. Both are plain paths and
circles — no fonts, no scripts, no external references — so they render anywhere the rest
of the project does, including inside the browser e2e pages.

| File | Use | Viewbox |
|---|---|---|
| `seqscribe-mark.svg` | README header, docs, anywhere with room for the wide form | 96 × 64 |
| `seqscribe-icon.svg` | favicon, avatar, anything square or below ~24px | 64 × 64 |

## What it depicts

Three writer-owned hash chains, each advancing left to right on its own, meeting in a
single larger head. That is the library's one idea in one picture: **convergence, not
consensus** — no stream gives up ownership to join, and the shared head is what every peer
agrees has *arrived*, never what they agreed to *do*. The dots are seq-numbered entries;
the lines between them are the chain.

## Two files, on purpose

The wide mark's three-column chain closes into a smudge below roughly 24px. The icon is
the same idea with two entries per stream and a shorter tail, which survives a 16px
favicon. Use the icon whenever the render is square or small.

## Colour

Both use `#7d8590`, a mid-grey that stays legible on white and on GitHub's `#0d1117`
without a media query. This matters because an `<img>` cannot inherit `color` from the page
— CSS does not reach inside a referenced SVG — so a `currentColor`-only file renders black
and disappears on dark backgrounds. Verified in both themes at 96/32/16px.

When **inlining** the SVG (pasting the markup into a page rather than referencing it),
swap the literal for `currentColor` and it will pick up the surrounding text colour
instead.

# Architecture Decision Records — 2026-09-03

Each ADR follows: Context → Decision → Consequences. Numbering continues
from wherever your repo's ADR log currently stands (renumber `ADR-00X` below
to match).

---

## ADR-00X: Fail loudly on incomplete comment pulls instead of silently truncating

**Context**
The initial ingestion script treated a `403` response (quota exceeded, or
comments disabled) as an end-of-pagination signal — it printed a message and
broke the loop, same as a normal "no more pages" exit. This meant a partial
pull due to a real error would land a JSON file indistinguishable from a
complete one.

**Decision**
A `403` mid-pagination now raises an exception instead of breaking silently.
Bronze data is only considered valid if the full pull completed without
error. No partial file should be treated as done.

**Consequences**
- Failed pulls now require investigation/retry rather than silently passing
  as complete.
- Slightly more operational overhead (need to handle/alert on the exception
  in the Airflow DAG), but this is the correct tradeoff — a wrong "polarization
  score" computed on partial data is worse than a visible pipeline failure.

---

## ADR-00X: Explicitly fetch full reply threads beyond the API's inline limit

**Context**
`commentThreads.list` only returns up to 5 replies inline per top-level
comment, even when `totalReplyCount` is higher. The initial script did not
check for this, meaning any thread with 6+ replies would silently lose data
with no indication anything was missing.

**Decision**
After the top-level pull, every thread is checked: if
`totalReplyCount > len(inlined replies)`, a follow-up call to
`comments.list` (filtered by `parentId`) fetches the full reply set for that
thread specifically.

**Consequences**
- Adds ~1 quota unit per thread with under-captured replies — cheap at this
  project's volume (10,000 units/day budget), but should be monitored if
  seed videos with very long, heavily-replied threads are added later.
- Bronze data now genuinely represents "all comments," not "all comments up
  to the API's default inline cap."

---

## ADR-00X: Restrict `videos.list` `part` parameters to snippet, statistics, contentDetails, status

**Context**
The YouTube Data API's `videos.list` endpoint supports many `part` values
(`brandPartner`, `player`, `topicDetails`, `fileDetails`, `processingDetails`,
`suggestions`, etc.), most of which are either irrelevant to this project,
empty for third-party videos, or only populated when authenticated as the
video's own channel owner.

**Decision**
Only pull `snippet,statistics,contentDetails,status` — the fields that map
directly to `dim_video`'s schema (title, published_at, duration, view_count)
plus status flags useful for catching videos that get taken down or
restricted mid-tracking.

**Consequences**
- Keeps quota cost minimal (`videos.list` is 1 unit flat regardless of parts
  requested, so this is more about avoiding dead weight in Bronze JSON than
  quota savings specifically).
- `categoryId` (inside `snippet`) is retained as a rough sanity-check field,
  but is explicitly NOT used for topic classification — YouTube's generic
  categories (e.g. "News & Politics") don't map to debate subjects like
  immigration vs. gun control. Topic assignment stays manual/seed-based per
  §7 of the project doc.

---

## ADR-00X: Add toxicity score as a second polarization signal, alongside sentiment

**Context**
The original polarization metric (project doc §5) used sentiment-score
variance alone. Prior research (Mall et al., "Politics on YouTube: Detecting
Online Group Polarization Based on News Videos' Comments") uses a toxicity
score (via Perspective API) as its core polarization signal instead, on the
reasoning that hostility is more directly tied to polarization than generic
valence, and expects a "U" or "M" shaped toxicity distribution for genuinely
polarizing content — the same bimodal-distribution logic already in this
project's design, applied to a different axis.

**Decision**
Add toxicity score (Perspective API, free, similar quota model to YouTube
Data API) as a second variable in `fact_comment_sentiment`, alongside
sentiment. Sentiment captures positive/negative; toxicity captures
hostility/civility. A topic can be negative-but-civil vs.
negative-and-toxic — these are meaningfully different polarization
signatures that sentiment alone can't distinguish.

**Consequences**
- One additional API integration and one additional score per comment to
  store/compute.
- Metric design is now backed by a directly comparable prior-research
  approach, not just an internally-invented heuristic — worth citing in the
  project writeup.

---

## ADR-00X: Keep like_count and reply_count as separate, uncorrelated signals

**Context**
Initial assumption was that likes and replies roughly track each other
(people who like a comment might also be more likely to reply to it). This
does not hold in practice — likes tend to signal passive agreement, while
replies are often used to contest or argue with a comment, meaning a
high-reply comment is frequently a *controversial* one, not a
broadly-agreed-with one.

**Decision**
Do not combine or assume correlation between `like_count` and
`totalReplyCount`. Store both as separate fields in
`fact_comment_sentiment`. Treat `like_count` as an agreement/consensus proxy
and `reply_count` as a controversy/contestation proxy.

**Consequences**
- A comment with high likes AND high replies becomes an interesting,
  distinguishable case (broadly endorsed AND actively contested) rather than
  being flattened into a single "engagement" score.
- The relationship (or lack thereof) between the two fields is itself a
  candidate chart/finding for the final writeup.

---

## ADR-00X: Derive debate-side/entity taxonomy from video metadata, not from comment-frequency clustering

**Context**
Comments reference debate participants using inconsistent, informal
descriptors ("red shirt guy," "bandana," first names) rather than
consistent identifiers. An initial proposal was to auto-discover these
aliases via frequency-based clustering of noun-phrase patterns across the
comment corpus. This has a real selection-bias problem: participants who
generate more/more intense reactions are more likely to be discovered by a
frequency threshold, meaning the entity list used to *measure* polarization
would already be pre-filtered by a proxy for polarization/reaction
intensity — a circularity risk.

**Decision**
Where available, participant identity (name, handle, side) is parsed from
video description / pinned comment — an external, channel-published ground
truth independent of comment volume. Comment text is then matched *against*
this known list (informal aliases → known participant), rather than used to
*discover* the list in the first place. Comment-frequency clustering is
retained only as a fallback enrichment layer for descriptor→name mapping
(via co-occurrence, e.g. a comment using both "red shirt guy" and a real
name together), not as the primary discovery mechanism.

**Consequences**
- Requires per-channel/per-video checking of whether a parseable participant
  list actually exists in the description or pinned comment (coverage is
  not guaranteed across all seed videos — confirmed to be inconsistent
  during initial channel review).
- Where no such list exists, this ADR's fallback (comment-based discovery)
  re-introduces the selection-bias risk described above — any polarization
  result relying on the fallback path should disclose this as a limitation.
- Avoids building a system that infers or tracks identity of real
  individuals from video frames/appearance — entity resolution stays
  entirely text-based, using only information the channel itself published.

---

## ADR-00X: Apply entity/alias clustering conditionally, based on per-side participant count

**Context**
Not all debate formats need the same level of entity-resolution effort.
1-on-1 formats (e.g. Open to Debate) only require binary side
classification — any consistent descriptor unambiguously maps to one of two
people. Multi-participant-per-side formats (e.g. Jubilee's "Surrounded,"
1-vs-20) require actual within-side disambiguation, since a vague
descriptor could refer to any of several people on the same side.

**Decision**
Entity clustering is triggered per-video based on structure, not total
headcount: if any single side has more than one participant, full
alias/clustering logic applies to that side. If every side has exactly one
participant, simple binary/positional classification is used instead,
regardless of total participant count across the video.

**Consequences**
- Avoids applying the heaviest resolution logic uniformly to every video
  when it's only structurally necessary for some.
- Requires a per-video/per-format flag (`participants_per_side`) to be
  captured during seeding, likely as an addition to `dim_channel` or a new
  per-video config alongside the existing `format` field.

---

## ADR-00X: Diversify seed channels beyond Jubilee for cross-publisher comparison

**Context**
The project's original research question (§1) asks whether different
channels/formats produce different polarization on the same topic. Initial
seed research found the debate-video space to be heavily concentrated under
one publisher (Jubilee), across multiple sub-formats (Middle Ground,
Surrounded, Spectrum, etc.) — sourcing exclusively from Jubilee would only
allow a within-publisher, cross-format comparison, not the cross-publisher
comparison originally scoped.

**Decision**
Add at least one non-Jubilee channel explicitly positioned as
non-partisan/balanced (e.g. Open to Debate, formerly Intelligence Squared
U.S.) to the seed list, to enable genuine cross-publisher comparison. If a
suitable second publisher at comparable comment volume can't be found, the
research question is explicitly reframed as "across Jubilee's own formats"
rather than silently treated as cross-publisher when it isn't.

**Consequences**
- Different channels use different description/participant-listing
  conventions, so the metadata-parsing logic (see prior ADR) needs to
  tolerate format variation across publishers, not just across videos
  within one channel.
- A channel's self-described "neutrality" is not taken at face value —
  spot-checked against a sample of its actual topics/participant selection
  before committing seed quota.

---

## ADR-00X: Accept partial automated coverage for participant identification, with a disclosed unresolved rate, instead of forcing full automation

**Context**
Commenters frequently identify debate participants by purely visual
descriptors ("bandana guy," "red shirt dude," "African brother") rather than
by name or stated side. This surfaced through several stages of the same
underlying problem:

1. Initial proposal: auto-discover participant aliases via frequency-based
   clustering of descriptor phrases across the comment corpus. Rejected —
   this makes participants who provoke more/stronger reactions more likely
   to be discovered, meaning the entity list used to *measure* polarization
   would be pre-filtered by a proxy for polarization itself (circularity).
2. Revised proposal: anchor side/entity taxonomy on external,
   channel-published sources instead — video description, pinned comment,
   auto-caption self-identification ("I'm unhoused..."), and on-screen
   graphic OCR (reading name/title lower-thirds). This closes the
   circularity problem and is fully automatable, but does not cover
   purely visual descriptors, since none of these sources contain
   information about what a participant looks like or is wearing.
3. A further proposal to close that remaining gap — using comment text
   that both praises/criticizes a participant AND uses a visual descriptor
   to infer identity (e.g. "red shirt guy really hates poor people" →
   infer he's the wealthy side) — was rejected. This uses *evaluative,
   opinion-laden* comment text to build the same taxonomy that sentiment
   is later measured against, reintroducing circularity one level deeper
   than the original clustering proposal.
4. A proposal to close the gap via automated frame extraction / visual
   recognition (screenshotting the video to match descriptors like
   "bandana" or "red shirt" to a specific real person, at scale, across
   many videos, without human review) was rejected. This would constitute
   an automated appearance-based identification/tracking system applied
   to real, identifiable individuals — a materially different and riskier
   category of system than text-based entity matching, regardless of
   research intent, and separately raises YouTube ToS concerns around
   automated capture of video content at scale.

**Decision**
Participant/side identification runs as a two-tier pipeline:

- **Tier 1 (fully automated, always available):** description parsing,
  pinned-comment parsing, caption-based self-identification, and
  on-screen-graphic OCR. Comments are matched against this list using only
  *identifying* phrasing (naming, labeling, physical description stated
  neutrally) — not *evaluative* phrasing about a participant, even when it
  co-occurs with a known descriptor.
- **Tier 2 (manual, one-time, per video):** a lightweight tagging pass —
  reviewing the small set of high-frequency descriptors Tier 1 couldn't
  resolve (candidates surfaced automatically from comment-frequency
  analysis, not rewatching the full video) and assigning them to a known
  side/participant by hand. This is done once per video and cached
  permanently against that video's ID — not repeated per comment, per
  analysis run, or per future user/request.

Every result reports an **unresolved rate** (% of side-referencing comments
that could not be matched via Tier 1) alongside the metric itself, rather
than silently treating unmatched comments as neutral or guessing a side for
them.

**Consequences**
- The pipeline never claims 100% automated resolution — this is treated as
  an honest, expected property of the problem (comparable to entity
  resolution/record linkage gaps in industry systems generally), not a
  defect to hide.
- A video's polarization score is available immediately at partial (Tier 1)
  coverage, with a disclosed confidence/coverage figure; full coverage is
  available once Tier 2 tagging is done for that video, once, ever.
- If this project becomes a public-facing tool, the same caching principle
  applies at product scale: Tier 2 tagging is a per-new-video cost, not a
  per-user-request cost, and could later be crowdsourced (surfaced to
  users) rather than done solely by the project owner.
- Before treating the Tier-1-only score as reliable, the unresolved bucket
  should be spot-checked against the resolved bucket (e.g. compare average
  sentiment) to confirm the gap looks like noise rather than a systematic
  bias — if the two buckets diverge meaningfully, the partial score should
  be caveated more strongly in any reported result.
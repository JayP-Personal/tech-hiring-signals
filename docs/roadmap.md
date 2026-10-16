# Roadmap

Each milestone becomes a GitHub milestone and ends with a tagged release. Each checkbox is sized as one issue and one pull request. Task prefixes follow [Conventional Commits](https://www.conventionalcommits.org/) and are the PR titles.

**Guiding constraint:** postings disappear when they close and can't be backfilled, so the collector goes live first and everything else is built while it runs.

---

## v0.2 — Ingestion

**Goal:** a daily Airflow DAG lands every tracked company's job board in Snowflake, reliably and unattended.

### Company list
Companies are chosen from published lists, then matched to their job boards using a
community inventory and verified against each board's live API (see ADR 0003).

- [x] `docs(adr)`: 0003 company sampling frame (source lists, inclusion rules, frozen core cohort, how boards are found and verified)
- [x] `feat(companies)`: candidates file built from the source lists, one row per company, tagged with stratum and source list
- [x] `feat(companies)`: board-matching script (name match against the ats-scrapers inventory, then slug guessing), with unit tests for name normalization and matching
- [x] `feat(companies)`: live verification step (board responds, has open postings, postings belong to the right company)
- [x] `docs(companies)`: coverage report (candidates vs. supported boards, by stratum and platform)
- [x] `feat(seeds)`: freeze the core cohort into the dbt seed (platform, slug, stratum, source list, added_on, status)

### Collection
- [x] `feat(snowflake)`: raw table for job board responses (`VARIANT` payload, company, platform, board slug, fetched_at)
- [x] `feat(ingest)`: common client interface and Greenhouse client with retries, rate limiting and tests
- [x] `feat(ingest)`: Lever client with tests
- [x] `feat(ingest)`: Ashby client with tests
- [ ] `feat(airflow)`: daily DAG that loads all boards into Snowflake, plus a DAG import test
- [ ] `feat(airflow)`: failure alerting
- [ ] `chore(release)`: v0.2.0

**Done when:** the DAG has run 7 days in a row without manual intervention, and the
coverage report states what share of each stratum the sample covers.

---

## v0.3 — Staging

**Goal:** one clean, tested table of postings regardless of which platform they came from.

- [ ] `feat(dbt)`: sources with freshness tests on the raw table
- [ ] `feat(dbt)`: one staging model per platform, flattening JSON
- [ ] `feat(dbt)`: unified `stg_postings` across platforms
- [ ] `feat(dbt)`: deduplication of multi-location and reposted listings
- [ ] `docs(adr)`: how a "posting" is defined (and what counts as a repost)
- [ ] `chore(release)`: v0.2.0

**Done when:** every model has a stated grain, a primary-key test and column descriptions.

---

## v0.4 — Posting lifecycle

**Goal:** know when each posting opened, changed and closed.

- [ ] `feat(dbt)`: snapshot of postings to capture edits (title, salary, remote status)
- [ ] `feat(dbt)`: `fct_posting_lifecycle` with first seen, last seen, closed flag
- [ ] `feat(dbt)`: `fct_collection_gaps` (expected vs. observed fetches per board per day)
- [ ] `docs(adr)`: handling postings already open when collection began (left truncation) and still open at analysis time (right censoring)
- [ ] `chore(release)`: v0.3.0

---

## v0.5 — Enrichment

**Goal:** turn free-text postings into analyzable features.

- [ ] `feat(dbt)`: title normalization into role families (data, ML, software, analyst, …)
- [ ] `feat(dbt)`: seniority level from title
- [ ] `feat(dbt)`: remote / hybrid / onsite
- [ ] `feat(dbt)`: salary range parsing where disclosed
- [ ] `feat(skills)`: keyword dictionary for skill extraction, with AI/LLM skills flagged
- [ ] `analysis(skills)`: hand-label ~200 postings; measure extractor precision and recall
- [ ] `feat(dbt)`: `mart_postings_daily` analysis panel
- [ ] `chore(release)`: v0.4.0

---

## v0.6 — Analysis

**Goal:** answer the question, with honest uncertainty.

- [ ] `analysis(ai-demand)`: share of postings requiring AI skills over time, by role family and seniority, with confidence intervals
- [ ] `analysis(time-to-close)`: Kaplan–Meier curves by role family
- [ ] `analysis(time-to-close)`: Cox proportional hazards model; check its assumptions
- [ ] `analysis`: robustness checks (sampling frame, extractor error, definition choices)
- [ ] `docs`: `analysis/reports/findings.md` — methods, results, uncertainty, limitations
- [ ] `chore(release)`: v0.5.0

---

## v1.0 — Presentation

- [ ] `feat(tableau)`: dashboard published to Tableau Public; screenshots in `docs/images/`
- [ ] `docs`: README rewritten around the findings
- [ ] `docs`: external writeup (blog or LinkedIn) linked from README
- [ ] `chore(release)`: v1.0.0

---

## Later / ideas

- Combine with state WARN Act layoff notices: which companies cut in one area while hiring in another?
- Salary trends under pay-transparency laws
- Forecasting posting volume, with backtesting
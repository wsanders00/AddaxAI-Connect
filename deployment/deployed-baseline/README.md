# Deployed application baseline — 2026-11-17

This directory reconciles the fork to the observed production application code.
It is a source checkpoint, not an upgrade or a production deployment instruction.
The upstream base is `v0.16.0`, commit
`5eded40f2ad1de7495520982be232f806827db5f`.

## What is actually deployed

| Component | Observed application code |
| --- | --- |
| Pi API | Upstream v0.16, plus the retained pipeline-recovery and archive-intake migrations and their no-op merge migration. All 224 collected source/template files match this tree. |
| Pi frontend | The exact v0.16 image recorded at the October 15 cutover is still running. Compiled asset hashes are in the manifest. Source is the retained upstream tag; no frontend rebuild was performed here. |
| BatDuck bulk worker | The fork's durable bulk outcomes, recovery, explicit archive-target handling and session fix. Its shared models and vendored ingestion helpers are preserved separately. |
| BatDuck detection worker | The fork's worker/recovery code and its deployed shared-library variant. |
| BatDuck SpeciesNet worker | The fork's worker/recovery code, plus the deployed country/US-state normalization adapter. The previously dirty local classifier was byte-identical to the live classifier. |

`manifest.json` records the running image IDs and maps every collected file's
container path to its repository source and SHA-256. The live comparison covered
407 source/template files across API and three workers, plus frontend asset
hashes. Collection excluded environment files, credentials, media, model weights,
installed third-party packages and caches. Private raw evidence remains outside
Git in the original checkout's ignored `data/review/deployed-reconciliation-20261117/`.

## Preserve the component boundaries

Production does not use one version of `shared/` everywhere:

- The root `shared/` tree is the API's upstream v0.16 library.
- `overlays/worker-common/shared/` contains the deployed worker differences.
- `overlays/bulk-upload/shared/` preserves the bulk worker's distinct models.
- `overlays/bulk-upload/ingestion/` preserves its two vendored ingestion differences.
- The upstream API-only `privacy.py` is excluded from the worker profiles.

The alternative Containerfiles apply these overlays in order. Ordinary upstream
worker Dockerfiles alone do **not** reproduce the deployed source combination.
For an isolated build review, the build-only Compose override is:

```sh
docker compose -f docker-compose.yml \
  -f deployment/deployed-baseline/compose.build.yml config
```

This is not the production topology. Rootless Podman units, Pi/BatDuck placement,
GPU settings, networks, object storage, credentials and activation remain owned
by the neighboring `cross-vlan-services` project. Do not run Compose `up` against
production based on this checkpoint.

These profiles reconstruct application source, not bit-identical images. They
retain upstream dependency recipes and a mutable Python base. The live detection
and SpeciesNet images additionally have the October 19 one-file libc repair;
its exact image lineage, package verification and Containerfiles remain owned by
`cross-vlan-services/config/addaxai/worker-libc-repair-20261019/`. Rebuilding or
upgrading requires dependency/runtime validation and staged testing first.

## Verify this checkpoint

```sh
python3 deployment/deployed-baseline/verify.py
```

The verifier checks file hashes, the source COPY order of each profile, absence
of extra shared Python modules, Python syntax, and the single migration head
`20261014_preserve_archive_merge`. Optional `--inventory PATH` arguments compare
against the private observations collected from the running containers. All
checks passed with both live inventories on November 17. No images were rebuilt,
no migrations executed, and no production service or data was changed.

## Work retained outside this baseline

The older fork's custom health API/UI and custom bulk API/UI are not in the live
v0.16 API source and are not silently carried into this checkpoint. Their history
remains in Git. The separate reclassification utility and its unfinished tests
remain local work; they were not observed in the production containers.

The RC23 repeatable importer, video preparation, original retention, provenance,
timestamp exceptions and import ledgers live in the Wildlife project. They are
active external integrations, not missing Addax application files to copy here.
Its current run is complete except for four held corrupt videos. Its general
terminal-attempt retry defect remains a separate follow-up.

Future upstream integration should start from this reconciled baseline, audit
which worker customizations upstream replaces, preserve migration ancestry and
existing annotations, and validate the external importer's exact-source/schema
contract. Do not assume the older fork README or a shared VERSION string proves
which code any particular deployed component runs.

## Upgrade assessment — November 17, 2026

**Direction superseded by the owner's stock-first decision:** do not automatically
port these worker patches. An isolated, unmodified v0.21.2 dev instance is now
running on BatDuck. Initial HTTP upload/resume/duplicate tests passed; explicit
historical camera/deployment targeting is a confirmed API gap. The importer
remains separate and production remains on the reconciled baseline. Deployment
and acceptance evidence are owned by the neighboring infrastructure project at
`cross-vlan-services/config/addaxai/stock-dev-20261117/README.md`. The assessment
below records the earlier patch-preserving option, not the current execution plan.

The reconciled baseline was published to the fork's `development` branch at
`defe2e09` (source checkpoint `bfa875f9`). Production was not changed.
The candidate assessed is upstream **v0.21.2**, commit
`dc7223ddf6e618d316529cc55220dcf4038074dd`, verified against GitHub's release API
and fetched tags. The cached web release listing lagged behind the API.

### What our fork actually changes

| Change | Observed deployment status | Upgrade treatment |
| --- | --- | --- |
| Database-backed worker claims, leases, bounded retry and stale-work recovery | Present in detection, SpeciesNet and bulk worker sources | Preserve the contract while porting to the new worker architecture. |
| Guarded detection/classification writes that reuse existing results on redelivery | Present in deployed worker sources; protects existing verification records | Retain and test crash-after-commit and duplicate-delivery cases. |
| Durable per-file bulk outcomes, transaction-linked image creation, staged-file checks and classification-aware completion | Present in deployed bulk worker and its shared/ingestion overlays | Upstream upload resume is not equivalent to this durable worker ledger. |
| Bulk progress heartbeat tied to completed work | Present in deployed bulk worker | Keep useful liveness semantics when integrating new queue/model telemetry. |
| Bulk validation up to 50 MiB | Present in deployed bulk worker | Upstream now has a shared 50 MB bulk limit; prefer its implementation after boundary tests. |
| SpeciesNet country/state normalization, including `US-CA` to `CA` | Present in deployed classifier | Upstream implements state conversion in its new SpeciesNet module; compare validation and inference behavior before dropping ours. |
| Archive target, source-hash and timestamp/provenance checks; session-lifetime fix | Code retained in bulk worker, schema retained in API | Legacy archive API/UI are absent from the deployed API/frontend. Do not describe this as an active end-to-end archive interface or re-enable it implicitly. |
| Custom health API/UI and bulk API/UI | Historical fork work, absent from deployed v0.16 API/frontend | Remain in Git history; not automatically carried into an upgrade. |
| Reclassification utility and tests | Unfinished local files, not observed deployed | Remain outside the published baseline. |

Some differences are older component code, not deliberate features: for example,
the bulk worker's vendored ingestion helper lacks the newer retired-camera
filters, and worker shared libraries predate the API's privacy module. The
checkpoint preserves what runs; these differences need explicit review rather
than automatic preservation as requirements. The external RC23 importer and
container libc repair remain separate from application customization.

### Benefits in the newer releases

- [v0.17.0](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.17.0): per-project SpeciesNet geography/taxonomy and revised inference behavior.
- [v0.18.0](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.18.0): per-project model selection and a unified classification service.
- [v0.19.0](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.19.0) and [v0.20.0](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.20.0): transport/password hardening and optional two-factor authentication.
- [v0.21.0](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.21.0): SD/live duplicate detection, resumable uploads, clearer skips and longer authenticated sessions.
- [v0.21.1](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.21.1) and [v0.21.2](https://github.com/PetervanLunteren/AddaxAI-Connect/releases/tag/v0.21.2): faster image delivery and notification fixes.

### Required integration and validation

1. Start an isolated integration branch from this baseline targeting v0.21.2.
   Keep this manifest as the historical deployed checkpoint; do not overwrite
   its hashes to make upgraded code appear equivalent to the old deployment.
2. Reconcile migrations. Simply combining the trees leaves our
   `20261014_preserve_archive_merge` and upstream `20261112_two_factor` as two
   heads. Add a reviewed merge revision; do not rewrite applied revisions.
   Rehearse migration on a restored database copy, checking custom columns,
   annotations, image counts and project model/geography/taxonomy assignments.
   The latest country/taxonomy migrations copy server data before dropping old
   fields; skip the intermediate v0.17 deployment. Model backfill uses taxonomy
   presence, so verify each real project's resulting model explicitly.
3. Port recovery and guarded writes into `services/classification`, detection,
   and bulk. Reconcile the shared model definitions with the new schema rather
   than reusing old full-file overlays. Test duplicate delivery, worker death,
   lease expiry, partial uploads, cancellation and manual verification retention.
4. Validate the external RC23 importer's exact-source/schema contract and its
   receipts, provenance, duplicate handling and notification suppression.
   Upstream removes the legacy ZIP/inspect queue and changes accepted bulk
   extensions; review these against actual clients. Do not run the optional
   duplicate-removal script against production as part of this assessment.
5. Build and test Pi ARM64 control-plane and BatDuck GPU images. Adapt the
   unified classifier, model cache/memory requirements, credentials/config,
   security settings and health checks to the owned Podman deployment. Review
   the libc repair against the new base/dependencies. Upstream API image builds
   now use a BuildKit `.git` bind mount for commit identity; verify compatibility
   with our build path. Stop old classifier consumers during eventual cutover.
6. Verify a current database/media backup and perform a restore rehearsal.
   Stage a representative import and classification sample, including US-state
   inference and existing human labels. Plan a coordinated service cutover and
   rollback using old images plus the matching database backup; migration
   downgrade is not a substitute for restore.

Recommendation: target v0.21.2 directly in an isolated integration branch. Its
upload, inference and security improvements are useful, but source merge success
alone would not establish readiness. This assessment did not merge upstream,
build images, run migrations, modify production, or establish an upgrade ETA.

## Separate AddaxAI Desktop trial

The official Desktop v7.20.0 Linux package ran in rootless Podman with its bundled
backend and a private reverse proxy. In the browser, a CPU job analyzed one
photo and one original H.264 clip. The clip produced one deer track across 44
sampled frames. Playback, seeking and the box on the selected frame worked.
The frame review and a manually confirmed count survived container recreation.
The spreadsheet export wrote six CSV tables, including Files, Counts and Frames.
[Upstream Desktop release](https://github.com/PetervanLunteren/AddaxAI/releases/tag/v7.20.0)

One camera recorded its photo in local time and its video creation timestamp in
UTC. Desktop treated the video time as local even with the project timezone set.
It separated the two files into different events and exported the five-hour
error. Reading the video timestamp as UTC reproduced the camera overlay time.

A follow-up tested two photo/video pairs from the same registered camera, one
in standard time and one in daylight time. The standalone preparer's time
resolver supplied the local clip start. Writing it to the movie creation field
on separate full-length Desktop copies preserved both video tracks and the audio
packet for packet. Stock Desktop grouped each pair into one event and exported
one white-tailed deer per event. The Files, Counts and Frames tables agreed with
the corrected times. These copies carry a Desktop-specific interpretation of
the movie creation time. Keep the original UTC metadata as the archive record
and verify other camera clocks before wider use. GPU inference remains untested
in this Desktop trial.

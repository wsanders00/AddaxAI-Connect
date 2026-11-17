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

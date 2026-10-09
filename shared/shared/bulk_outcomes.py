"""Pure helpers for the durable per-index bulk upload outcome ledger."""

from collections import Counter


TERMINAL_FILE_OUTCOMES = {"classified", "failed", "duplicate", "skipped"}
CLIENT_FAILURE_REASONS = {"upload_failed", "request_too_large", "cancelled"}


def ledger_from_manifest(manifest):
    """Return a detached index-keyed ledger, tolerating old manifest shapes."""
    ledger = (manifest or {}).get("upload_ledger") or {}
    if isinstance(ledger, dict) and ledger:
        return {str(key): dict(value) for key, value in ledger.items() if isinstance(value, dict)}
    # Older completed imports retain per-file evidence in file_log. Reconcile
    # it for display/recovery without rewriting their stored expected counts.
    legacy = (manifest or {}).get("file_log") or []
    recovered = {}
    if isinstance(legacy, list):
        for index, entry in enumerate(legacy):
            if not isinstance(entry, dict):
                continue
            outcome = entry.get("outcome")
            if outcome not in {"processed", "queued", "classified", "duplicate", "skipped", "failed"}:
                continue
            item = dict(entry)
            item["outcome"] = "queued" if outcome == "processed" else outcome
            item["accepted"] = True
            recovered[str(index)] = item
    return recovered


def merge_ledger_entry(manifest, index, entry, *, protect_accepted=True):
    """Merge one server outcome without allowing client reports to erase acceptance."""
    updated = dict(manifest or {})
    ledger = ledger_from_manifest(updated)
    key = str(index)
    prior = ledger.get(key, {})
    if protect_accepted and prior.get("accepted"):
        return updated, False
    merged = dict(prior)
    merged.update(entry)
    ledger[key] = merged
    updated["upload_ledger"] = ledger
    return updated, True


def summarize_ledger(manifest, total_files):
    """Count upload acceptance, terminal outcomes, and unresolved indexes."""
    ledger = ledger_from_manifest(manifest)
    counts = Counter()
    missing = []
    for index in range(total_files):
        entry = ledger.get(str(index))
        if entry is None:
            missing.append(index)
            continue
        if entry.get("accepted"):
            counts["uploaded_files"] += 1
        outcome = entry.get("outcome")
        if outcome in TERMINAL_FILE_OUTCOMES:
            counts[f"{outcome}_files"] += 1
        elif outcome in {"failed_upload", "failed"}:
            counts["failed_files"] += 1
        else:
            counts["pending_files"] += 1
    counts["pending_files"] += len(missing)
    counts["missing_index_count"] = len(missing)
    counts["missing_indexes"] = missing
    return dict(counts)


def terminal_status(summary):
    """Choose an honest terminal state after all accepted files resolve."""
    unresolved = summary.get("pending_files", 0)
    classified = summary.get("classified_files", 0)
    duplicates = summary.get("duplicate_files", 0)
    failed = summary.get("failed_files", 0)
    skipped = summary.get("skipped_files", 0)
    if unresolved:
        return None
    if failed or skipped:
        return "partial" if classified or duplicates else "failed"
    return "done" if classified or duplicates else "failed"

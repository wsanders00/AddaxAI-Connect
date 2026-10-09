/**
 * Bulk-upload runtime store
 *
 * One active client-side upload session per project. The store owns
 * both the live state (counts, started-at, cancellation flag) and
 * the actual upload loop, so the modal can close right after the
 * user clicks Upload without losing progress. The job row in the
 * list page subscribes to this store to render live counts; once
 * the upload finishes and the worker takes over, the row falls
 * back to the server-recorded `processed_files`.
 *
 * Why module-level: the upload loop must survive React unmounts
 * (modal close, navigation away from the bulk-upload page). React
 * state can't do that, refs go with the component. Zustand outside
 * React gives us a singleton that lives for the lifetime of the
 * page.
 */
import { create } from 'zustand';
import {
  bulkUploadApi,
  type BulkUploadJob,
  type BulkUploadManifest,
  type UploadFailureOutcome,
  type UploadFailureReason,
} from '../api/bulkUpload.ts';
import type { ScanEntry } from '../workers/bulkScanWorker';

const UPLOAD_CONCURRENCY = 4;
const UPLOAD_RETRIES = 3;

export interface ActiveUpload {
  jobUuid: string;
  projectId: number;
  // total = number of files we'll actually send. Excludes scan
  // skips (corrupt / missing-EXIF / safe-duplicates).
  total: number;
  uploaded: number;
  // Files already on the server when we started (resume case, or
  // pre-flight dedup hits). Counted into "done" so the bar and
  // percent reflect what's left for THIS session.
  skipped: number;
  /** Upload attempts that failed, separate from already uploaded indexes. */
  uploadFailed: number;
  startedAt: number;
  // Set when the upload loop has finished sending everything and
  // called finalize. The store keeps the row "live" for a beat
  // afterwards so the UI shows the success state before going
  // back to server-recorded progress.
  done: boolean;
  // Set by cancelActive(). The loop reads it on each iteration
  // and exits cleanly. Cancellation outcomes are persisted before the
  // server marks the job cancelled.
  cancelled: boolean;
  // Set if the upload loop hit an unrecoverable error (createJob,
  // finalize, hash-check). The row falls back to the server-
  // recorded state, which will surface the failure too.
  errored: boolean;
}

interface BeginNewArgs {
  projectId: number;
  folderName: string;
  // Exactly one of deviceId (Mode A, profile-matched camera) or siteId
  // (Mode B, manual site + synthetic camera).
  deviceId?: string;
  siteId?: number;
  manifest: BulkUploadManifest;
  excludedCapturedAts: string[];
  // Camera clock correction, already applied to manifest and entries.
  timeOffsetSeconds: number;
  files: File[];
  entries: ScanEntry[];
  onError: (msg: string) => void;
  onSuccess: () => void;
  onCacheInvalidate: () => void;
}

interface BeginResumeArgs {
  projectId: number;
  resumeJob: BulkUploadJob;
  files: File[];
  entries: ScanEntry[];
  onError: (msg: string) => void;
  onSuccess: () => void;
  onCacheInvalidate: () => void;
}

interface State {
  active: ActiveUpload | null;
}

interface Actions {
  beginNew: (args: BeginNewArgs) => void;
  beginResume: (args: BeginResumeArgs) => void;
  cancelActive: () => void;
  clear: () => void;
}

export const useBulkUploadStore = create<State & Actions>((set, get) => ({
  active: null,
  beginNew: (args) => {
    if (get().active && !get().active!.done && !get().active!.cancelled) {
      args.onError('An upload is already in progress, wait for it to finish.');
      return;
    }
    void runNewUpload(args, set, get);
  },
  beginResume: (args) => {
    if (get().active && !get().active!.done && !get().active!.cancelled) {
      args.onError('An upload is already in progress, wait for it to finish.');
      return;
    }
    void runResumeUpload(args, set, get);
  },
  cancelActive: () => {
    const active = get().active;
    if (!active || active.done) return;
    set({ active: { ...active, cancelled: true } });
  },
  clear: () => set({ active: null }),
}));

function makeValidEntries(entries: ScanEntry[], excluded: Set<string>): ScanEntry[] {
  return entries
    .filter(
      (e) =>
        e.status === 'valid'
        && !(e.captured_at && excluded.has(e.captured_at)),
    )
    .sort((a, b) => a.relative_path.localeCompare(b.relative_path));
}

async function runNewUpload(
  args: BeginNewArgs,
  set: (partial: Partial<State>) => void,
  get: () => State & Actions,
) {
  const excluded = new Set(args.excludedCapturedAts);
  const validEntries = makeValidEntries(args.entries, excluded);
  const total = validEntries.length;
  const totalBytes = validEntries.reduce((sum, e) => sum + e.size, 0);

  let job: BulkUploadJob;
  try {
    job = await bulkUploadApi.createJob(args.projectId, {
      folder_name: args.folderName,
      device_id: args.deviceId,
      site_id: args.siteId,
      total_files: total,
      total_bytes: totalBytes,
      manifest: args.manifest,
      time_offset_seconds: args.timeOffsetSeconds,
    });
  } catch (err: any) {
    args.onError(
      `Failed to create job, ${err.response?.data?.detail || err.message}`,
    );
    return;
  }

  set({
    active: {
      jobUuid: job.uuid,
      projectId: args.projectId,
      total,
      uploaded: 0,
      skipped: 0,
      uploadFailed: 0,
      startedAt: Date.now(),
      done: false,
      cancelled: false,
      errored: false,
    },
  });
  args.onCacheInvalidate();

  const upload = await runUploadLoop(args.projectId, job.uuid, args.files, validEntries, new Set(), set, get, args.onError, args.onCacheInvalidate);
  if (!upload) return;
  await finishOrCancel(args.projectId, job.uuid, upload, set, get, args.onSuccess, args.onError, args.onCacheInvalidate);
}

async function runResumeUpload(
  args: BeginResumeArgs,
  set: (partial: Partial<State>) => void,
  get: () => State & Actions,
) {
  const validEntries = makeValidEntries(args.entries, new Set());
  const total = validEntries.length;

  let alreadyUploaded: Set<number> = new Set();
  try {
    const indexes = await bulkUploadApi.uploadedIndexes(
      args.projectId,
      args.resumeJob.uuid,
    );
    alreadyUploaded = new Set(indexes);
  } catch (err: any) {
    args.onError(
      `Failed to read upload progress, ${err.response?.data?.detail || err.message}`,
    );
    return;
  }

  set({
    active: {
      jobUuid: args.resumeJob.uuid,
      projectId: args.projectId,
      total,
      uploaded: 0,
      skipped: alreadyUploaded.size,
      uploadFailed: 0,
      startedAt: Date.now(),
      done: false,
      cancelled: false,
      errored: false,
    },
  });
  args.onCacheInvalidate();

  const upload = await runUploadLoop(
    args.projectId,
    args.resumeJob.uuid,
    args.files,
    validEntries,
    alreadyUploaded,
    set,
    get,
    args.onError,
    args.onCacheInvalidate,
  );
  if (!upload) return;
  await finishOrCancel(
    args.projectId,
    args.resumeJob.uuid,
    upload,
    set,
    get,
    args.onSuccess,
    args.onError,
    args.onCacheInvalidate,
  );
}

async function runUploadLoop(
  projectId: number,
  jobUuid: string,
  files: File[],
  validEntries: ScanEntry[],
  alreadyUploaded: Set<number>,
  set: (partial: Partial<State>) => void,
  get: () => State & Actions,
  onError: (msg: string) => void,
  onCacheInvalidate: () => void,
): Promise<UploadBatchResult | null> {
  const queue = validEntries
    .map((e, i) => ({ entry: e, position: i }))
    .filter((row) => !alreadyUploaded.has(row.position));
  const current = () => get().active;
  const isCancelled = () => Boolean(current()?.cancelled);
  const updateCounts = (uploaded: number, failed: number) => {
    const a = get().active;
    if (a) set({ active: { ...a, uploaded, uploadFailed: failed } });
  };
  try {
    const result = await uploadFilesWithOutcomes(
      queue.map(({ entry, position }) => ({
        index: position,
        filename: entry.relative_path.split('/').pop() || entry.relative_path,
        file: files[entry.index],
      })),
      {
        upload: (index, file) => bulkUploadApi.uploadFile(projectId, jobUuid, index, file),
        reportOutcomes: async (outcomes) => {
          await bulkUploadApi.reportOutcomes(projectId, jobUuid, outcomes);
        },
        isCancelled,
        onProgress: updateCounts,
      },
    );
    return { ...result, previouslyUploaded: alreadyUploaded.size };
  } catch (err: any) {
    const active = current();
    if (active) set({ active: { ...active, errored: true, done: true } });
    onError(`Could not save upload outcomes. The job was not finalized; resume it to reconcile the files. ${errorDetail(err)}`);
    onCacheInvalidate();
    return null;
  }
}

interface UploadBatchResult {
  uploaded: number;
  failed: number;
  outcomes: UploadFailureOutcome[];
  cancelled: boolean;
  previouslyUploaded: number;
}

interface UploadBatchDependencies {
  upload: (index: number, file: File) => Promise<void>;
  reportOutcomes: (outcomes: UploadFailureOutcome[]) => Promise<void>;
  isCancelled: () => boolean;
  onProgress?: (uploaded: number, failed: number) => void;
  concurrency?: number;
  retries?: number;
  retryDelay?: (attempt: number) => Promise<void>;
}

/**
 * Production upload loop, kept independent of React so its retry and outcome
 * contract can be exercised directly by the Node test suite.
 */
export async function uploadFilesWithOutcomes(
  files: { index: number; filename: string; file: File }[],
  dependencies: UploadBatchDependencies,
): Promise<Omit<UploadBatchResult, 'previouslyUploaded'>> {
  const queue = [...files];
  const concurrency = dependencies.concurrency ?? UPLOAD_CONCURRENCY;
  const retries = dependencies.retries ?? UPLOAD_RETRIES;
  const retryDelay = dependencies.retryDelay
    ?? ((attempt: number) => new Promise<void>((resolve) => setTimeout(resolve, 500 * attempt)));
  const attempted = new Set<number>();
  const outcomes: UploadFailureOutcome[] = [];
  let cursor = 0;
  let uploaded = 0;
  let failed = 0;

  const worker = async () => {
    while (!dependencies.isCancelled()) {
      const next = queue[cursor];
      if (!next) return;
      cursor += 1;
      attempted.add(next.index);
      let failure: UploadFailureReason | null = null;
      for (let attempt = 1; attempt <= retries; attempt += 1) {
        try {
          await dependencies.upload(next.index, next.file);
          uploaded += 1;
          dependencies.onProgress?.(uploaded, failed);
          break;
        } catch (error) {
          if (dependencies.isCancelled()) {
            failure = 'cancelled';
            break;
          }
          if (uploadHttpStatus(error) === 413) {
            failure = 'request_too_large';
            break;
          }
          if (attempt === retries) {
            failure = 'upload_failed';
            break;
          }
          await retryDelay(attempt);
        }
      }
      if (failure) {
        outcomes.push({ index: next.index, filename: next.filename, reason: failure });
        if (failure !== 'cancelled') failed += 1;
        dependencies.onProgress?.(uploaded, failed);
      }
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  if (dependencies.isCancelled()) {
    for (const file of queue) {
      if (!attempted.has(file.index)) {
        outcomes.push({ index: file.index, filename: file.filename, reason: 'cancelled' });
      }
    }
  }
  outcomes.sort((a, b) => a.index - b.index);
  if (outcomes.length > 0) {
    // The response intentionally contains aggregate counts, not the private
    // per-index ledger, so the client cannot safely reassign an outcome here.
    await dependencies.reportOutcomes(outcomes);
  }
  return { uploaded, failed, outcomes, cancelled: dependencies.isCancelled() };
}

function uploadHttpStatus(error: any): number | undefined {
  return error?.response?.status ?? error?.status;
}

function errorDetail(error: any): string {
  const detail = error?.response?.data?.detail;
  if (typeof detail === 'string') return detail;
  if (detail && typeof detail === 'object' && typeof detail.message === 'string') {
    return detail.message;
  }
  return error?.message || 'Unknown error';
}

export function formatFinalizeError(error: any): string {
  const detail = error?.response?.data?.detail;
  if (error?.response?.status === 409 && detail && typeof detail === 'object') {
    const count = Number(detail.missing_index_count ?? detail.pending_files ?? 0);
    const indexes = Array.isArray(detail.missing_indexes)
      ? detail.missing_indexes.slice(0, 8).join(', ')
      : '';
    return `Upload could not be finalized: ${count} file${count === 1 ? '' : 's'} remain unresolved${indexes ? ` (indexes ${indexes})` : ''}. Resume the upload to reconcile them.`;
  }
  return errorDetail(error);
}

async function finishOrCancel(
  projectId: number,
  jobUuid: string,
  upload: Omit<UploadBatchResult, 'previouslyUploaded'> & { previouslyUploaded: number },
  set: (partial: Partial<State>) => void,
  get: () => State & Actions,
  onSuccess: () => void,
  onError: (msg: string) => void,
  onCacheInvalidate: () => void,
) {
  if (isCancelledNow(get)) {
    try {
      // Mark the job cancelled (keeps the row as "Cancelled", clears staging)
      // rather than removing it, so a stopped upload behaves like a stopped
      // processing job. The user can remove the row separately.
      await bulkUploadApi.cancel(projectId, jobUuid);
    } catch {
      // The row may already be gone, or the worker may have flipped
      // state. Either way the user asked to stop, do not block.
    }
    set({ active: null });
    onCacheInvalidate();
    return;
  }
  try {
    const finalized = await bulkUploadApi.finalize(projectId, jobUuid);
    onCacheInvalidate();
    if (finalized.status !== 'processing' || upload.failed > 0 || upload.outcomes.length > 0) {
      const failures = upload.outcomes.filter((item) => item.reason !== 'cancelled');
      const tooLarge = failures.filter((item) => item.reason === 'request_too_large').length;
      const otherFailed = failures.length - tooLarge;
      let message = `Upload outcome: ${upload.uploaded + upload.previouslyUploaded} files accepted and queued for analysis.`;
      const aggregateShowsAcceptedOutcome = upload.failed > 0
        && finalized.upload_failed_files !== undefined
        && finalized.upload_failed_files < upload.failed;
      if (aggregateShowsAcceptedOutcome) {
        message = 'Some upload responses could not be matched to per-file results. Check the recorded counts and resume any unresolved files.';
      } else if (tooLarge > 0) {
        message += ` ${tooLarge} file${tooLarge === 1 ? ' was' : 's were'} too large; reduce the file size and resume.`;
      }
      if (!aggregateShowsAcceptedOutcome && otherFailed > 0) {
        message += ` ${otherFailed} upload${otherFailed === 1 ? ' failed' : 's failed'}; resume to retry.`;
      }
      if (upload.cancelled) message += ' Remaining files were marked cancelled.';
      if (finalized.status === 'failed') {
        message = `${finalized.error_message || 'No files were accepted.'}${tooLarge > 0 ? ` ${tooLarge} file${tooLarge === 1 ? ' was' : 's were'} over the request limit; reduce its size before resuming.` : ''}`;
      }
      onError(message);
      const a = get().active;
      if (a && a.jobUuid === jobUuid) set({ active: { ...a, done: true, errored: true } });
      return;
    }
    const a = get().active;
    if (a && a.jobUuid === jobUuid) {
      set({ active: { ...a, done: true } });
    }
    onSuccess();
    onCacheInvalidate();
    // Hold the "done" state for a beat so the row shows the success
    // before falling back to server-recorded progress, then clear so
    // a follow-up upload can start.
    setTimeout(() => {
      const cur = get().active;
      if (cur && cur.jobUuid === jobUuid) set({ active: null });
    }, 3000);
  } catch (err: any) {
    onError(formatFinalizeError(err));
    const a = get().active;
    if (a && a.jobUuid === jobUuid) {
      set({ active: { ...a, errored: true, done: true } });
    }
  }
}

function isCancelledNow(get: () => State & Actions): boolean {
  return Boolean(get().active?.cancelled);
}

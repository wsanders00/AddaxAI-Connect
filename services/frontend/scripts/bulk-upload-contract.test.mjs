import test from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';

globalThis.__bulkUploadTestClient = {
  calls: [],
  post: async (...args) => {
    globalThis.__bulkUploadTestClient.calls.push(['post', ...args]);
    return { data: { status: 'processing' } };
  },
  get: async (...args) => {
    globalThis.__bulkUploadTestClient.calls.push(['get', ...args]);
    return { data: {} };
  },
  delete: async (...args) => {
    globalThis.__bulkUploadTestClient.calls.push(['delete', ...args]);
    return { data: {} };
  },
};
register('./bulk-upload-contract-loader.mjs', import.meta.url);

const {
  bulkUploadApi,
  bulkUploadOutcomeCounts,
  bulkUploadOutcomeText,
  bulkUploadStatusLabel,
  bulkUploadStatusTone,
  isBulkUploadSuccessful,
} =
  await import('../src/api/bulkUpload.ts');
const { useBulkUploadStore, formatFinalizeError } =
  await import('../src/lib/bulkUploadStore.ts');

function fileEntry(index, name) {
  return {
    index,
    relative_path: name,
    filename: name,
    status: 'valid',
    size: 5,
    captured_at: `2026-01-0${index + 1}T12:00:00`,
  };
}

async function waitFor(predicate) {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail('timed out waiting for production upload flow');
}

test('production API persists per-index outcomes and exposes image retry counts', async () => {
  const calls = globalThis.__bulkUploadTestClient.calls;
  calls.length = 0;
  await bulkUploadApi.reportOutcomes(7, 'job-uuid', [
    { index: 2, filename: 'large.jpg', reason: 'request_too_large' },
  ]);
  await bulkUploadApi.retryFailed(7, 'job-uuid');
  assert.deepEqual(calls, [
    ['post', '/api/projects/7/bulk-upload/jobs/job-uuid/outcomes', {
      files: [{ index: 2, filename: 'large.jpg', reason: 'request_too_large' }],
    }],
    ['post', '/api/projects/7/bulk-upload/jobs/job-uuid/retry-failed'],
  ]);
});

test('production job outcome helpers use response summaries and keep uploaded separate from classified', () => {
  const job = {
    status: 'partial', total_files: 8, uploaded_files: 7, processed_files: 4,
    classified_files: 4, failed_files: 2, upload_failed_files: 1,
    duplicate_files: 1, skipped_files: 0,
    pending_files: 1, outcome_warning: null,
    manifest: null,
  };
  assert.deepEqual(bulkUploadOutcomeCounts(job), {
    expected: 8, uploaded: 7, classified: 4, failed: 1, uploadFailed: 1,
    duplicates: 1, skipped: 0, pending: 1,
  });
  const completedWithDuplicates = {
    ...job, status: 'done', uploaded_files: 8, processed_files: 4,
    classified_files: 4, failed_files: 0, upload_failed_files: 0, duplicate_files: 3,
    skipped_files: 1, pending_files: 0,
  };
  assert.equal(isBulkUploadSuccessful(completedWithDuplicates), true);
  assert.match(bulkUploadOutcomeText(completedWithDuplicates), /Uploaded 8 · Classified 4/);
  assert.equal(isBulkUploadSuccessful({ ...job, status: 'done', outcome_warning: 'history unknown' }), false);
  assert.equal(isBulkUploadSuccessful({ ...job, status: 'done', pending_files: 1 }), false);
  const unknown = { ...job, status: 'done', total_files: 0, outcome_warning: 'history unknown' };
  assert.equal(bulkUploadStatusLabel(unknown), 'Outcome unknown');
  assert.equal(bulkUploadStatusTone(unknown), 'warning');
  assert.match(bulkUploadOutcomeText(unknown), /counts are unknown/);
  assert.equal(bulkUploadStatusLabel(job), 'Partial');
  assert.equal(bulkUploadStatusTone(job), 'warning');
  assert.match(bulkUploadOutcomeText(job), /Failed 1/);
  assert.match(bulkUploadOutcomeText(job), /Upload failed 1/);
});

test('production finalize error explains a 409 pending ledger and indexes', () => {
  const error = {
    response: {
      status: 409,
      data: { detail: { message: 'incomplete', pending_files: 2, missing_index_count: 2, missing_indexes: [3, 9] } },
    },
  };
  assert.match(formatFinalizeError(error), /2 files remain unresolved/);
  assert.match(formatFinalizeError(error), /indexes 3, 9/);
  assert.match(formatFinalizeError(error), /Resume/);
});

test('production store records 413 once before finalize and never signals success for a partial upload', async () => {
  const api = bulkUploadApi;
  const original = { ...api };
  const calls = [];
  Object.assign(api, {
    createJob: async () => ({ uuid: 'partial-job' }),
    uploadedIndexes: async () => [],
    uploadFile: async (_project, _job, index) => {
      calls.push(['upload', index]);
      if (index === 0) throw { response: { status: 413, data: { detail: 'request too large' } } };
    },
    reportOutcomes: async (_project, _job, outcomes) => {
      calls.push(['outcomes', outcomes]);
      return {};
    },
    finalize: async () => {
      calls.push(['finalize']);
      return { status: 'processing' };
    },
  });
  const errors = [];
  let success = 0;
  useBulkUploadStore.getState().clear();
  useBulkUploadStore.getState().beginNew({
    projectId: 1,
    folderName: 'card',
    manifest: { total_entries: 2, valid_count: 2, by_status: {}, date_range: { start: null, end: null } },
    excludedCapturedAts: [],
    timeOffsetSeconds: 0,
    files: [{}, {}],
    entries: [fileEntry(0, 'large.jpg'), fileEntry(1, 'small.jpg')],
    onError: (message) => errors.push(message),
    onSuccess: () => { success += 1; },
    onCacheInvalidate: () => {},
  });
  await waitFor(() => calls.some(([kind]) => kind === 'finalize'));
  assert.equal(calls.filter(([kind, index]) => kind === 'upload' && index === 0).length, 1, '413 is permanent');
  const outcomeIndex = calls.findIndex(([kind]) => kind === 'outcomes');
  const finalizeIndex = calls.findIndex(([kind]) => kind === 'finalize');
  assert.ok(outcomeIndex >= 0 && outcomeIndex < finalizeIndex, 'outcome persistence precedes finalization');
  assert.deepEqual(calls[outcomeIndex][1], [
    { index: 0, filename: 'large.jpg', reason: 'request_too_large' },
  ]);
  assert.equal(success, 0);
  assert.match(errors[0], /too large/);
  assert.match(errors[0], /resume/i);
  useBulkUploadStore.getState().clear();
  Object.assign(api, original);
});

test('production store blocks success and exposes missing indexes when finalize returns 409', async () => {
  const api = bulkUploadApi;
  const original = { ...api };
  const calls = [];
  Object.assign(api, {
    createJob: async () => ({ uuid: 'unresolved-job' }),
    uploadedIndexes: async () => [],
    uploadFile: async () => {},
    reportOutcomes: async () => ({}),
    finalize: async () => {
      calls.push('finalize');
      throw {
        response: {
          status: 409,
          data: { detail: { message: 'incomplete', pending_files: 2, missing_index_count: 2, missing_indexes: [1, 4] } },
        },
      };
    },
  });
  const errors = [];
  let success = 0;
  useBulkUploadStore.getState().clear();
  useBulkUploadStore.getState().beginNew({
    projectId: 1,
    folderName: 'card',
    manifest: { total_entries: 1, valid_count: 1, by_status: {}, date_range: { start: null, end: null } },
    excludedCapturedAts: [],
    timeOffsetSeconds: 0,
    files: [{}],
    entries: [fileEntry(0, 'one.jpg')],
    onError: (message) => errors.push(message),
    onSuccess: () => { success += 1; },
    onCacheInvalidate: () => {},
  });
  await waitFor(() => errors.length > 0);
  assert.deepEqual(calls, ['finalize']);
  assert.equal(success, 0);
  assert.match(errors[0], /2 files remain unresolved/);
  assert.match(errors[0], /indexes 1, 4/);
  useBulkUploadStore.getState().clear();
  Object.assign(api, original);
});

test('production store leaves a job unfinalized if failure outcomes cannot be persisted', async () => {
  const api = bulkUploadApi;
  const original = { ...api };
  const calls = [];
  Object.assign(api, {
    createJob: async () => ({ uuid: 'ledger-error-job' }),
    uploadedIndexes: async () => [],
    uploadFile: async () => { throw { response: { status: 413 } }; },
    reportOutcomes: async () => {
      calls.push('outcomes');
      throw new Error('ledger unavailable');
    },
    finalize: async () => { calls.push('finalize'); return { status: 'processing' }; },
  });
  const errors = [];
  let success = 0;
  useBulkUploadStore.getState().clear();
  useBulkUploadStore.getState().beginNew({
    projectId: 1,
    folderName: 'card',
    manifest: { total_entries: 1, valid_count: 1, by_status: {}, date_range: { start: null, end: null } },
    excludedCapturedAts: [],
    timeOffsetSeconds: 0,
    files: [{}],
    entries: [fileEntry(0, 'large.jpg')],
    onError: (message) => errors.push(message),
    onSuccess: () => { success += 1; },
    onCacheInvalidate: () => {},
  });
  await waitFor(() => errors.length > 0);
  assert.deepEqual(calls, ['outcomes']);
  assert.equal(success, 0);
  assert.match(errors[0], /not finalized/);
  useBulkUploadStore.getState().clear();
  Object.assign(api, original);
});

test('production store does not infer a per-file result from aggregate-only API responses', async () => {
  const api = bulkUploadApi;
  const original = { ...api };
  const calls = [];
  Object.assign(api, {
    createJob: async () => ({ uuid: 'accepted-response-lost-job' }),
    uploadedIndexes: async () => [],
    uploadFile: async () => { throw { response: { status: 413 } }; },
    reportOutcomes: async (_project, _job, outcomes) => {
      calls.push(['outcomes', outcomes]);
      return {
        status: 'uploading', total_files: 1, uploaded_files: 1,
        processed_files: 0, classified_files: 0, failed_files: 0,
        upload_failed_files: 0, duplicate_files: 0, skipped_files: 0,
        pending_files: 0, outcome_warning: null, manifest: null,
      };
    },
    finalize: async () => {
      calls.push(['finalize']);
      return {
        status: 'processing', total_files: 1, uploaded_files: 1,
        processed_files: 0, classified_files: 0, failed_files: 0,
        upload_failed_files: 0, duplicate_files: 0, skipped_files: 0,
        pending_files: 0, outcome_warning: null, manifest: null,
      };
    },
  });
  const errors = [];
  let success = 0;
  useBulkUploadStore.getState().clear();
  useBulkUploadStore.getState().beginNew({
    projectId: 1,
    folderName: 'card',
    manifest: { total_entries: 1, valid_count: 1, by_status: {}, date_range: { start: null, end: null } },
    excludedCapturedAts: [],
    timeOffsetSeconds: 0,
    files: [{}],
    entries: [fileEntry(0, 'accepted.jpg')],
    onError: (message) => errors.push(message),
    onSuccess: () => { success += 1; },
    onCacheInvalidate: () => {},
  });
  await waitFor(() => calls.some(([kind]) => kind === 'finalize') && errors.length > 0);
  assert.equal(success, 0, 'aggregate counts cannot prove which reported index was accepted');
  assert.match(errors[0], /could not be matched to per-file results/);
  useBulkUploadStore.getState().clear();
  Object.assign(api, original);
});

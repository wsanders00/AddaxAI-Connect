import test from 'node:test';
import assert from 'node:assert/strict';
import {
  pipelineActivity,
  serviceDisplayName,
  serviceStatusLabel,
  summarizeServices,
} from '../src/utils/healthPresentation.ts';

test('disabled services are excluded from the health denominator', () => {
  const summary = summarizeServices([
    { status: 'healthy' },
    { status: 'unhealthy' },
    { status: 'disabled' },
    { status: 'not_configured' },
  ]);

  assert.deepEqual(summary, {
    healthy: 1,
    unhealthy: 1,
    checked: 2,
    disabled: 2,
    allHealthy: false,
  });
});

test('all-disabled services do not imply that the system is healthy', () => {
  const summary = summarizeServices([{ status: 'disabled' }, { status: 'not_configured' }]);

  assert.equal(summary.checked, 0);
  assert.equal(summary.disabled, 2);
  assert.equal(summary.allHealthy, false);
});

test('real unhealthy checks remain failures even when disabled services are present', () => {
  const summary = summarizeServices([{ status: 'disabled' }, { status: 'unhealthy' }]);

  assert.equal(summary.unhealthy, 1);
  assert.equal(summary.checked, 1);
  assert.equal(summary.allHealthy, false);
});

test('service labels describe backend identity without baking in a provider', () => {
  assert.equal(serviceDisplayName('minio'), 'Object storage');
  assert.equal(serviceDisplayName('bulk-upload'), 'Bulk-upload worker');
  assert.equal(serviceStatusLabel('disabled'), 'Disabled');
  assert.equal(serviceStatusLabel('not_configured'), 'Not configured');
});

test('pending pipeline work is activity, not an unhealthy-service result', () => {
  assert.deepEqual(pipelineActivity(0), {
    label: 'Idle',
    message: 'No images pending classification',
  });
  assert.deepEqual(pipelineActivity(3), {
    label: 'Work pending',
    message: '3 images pending classification',
  });
});

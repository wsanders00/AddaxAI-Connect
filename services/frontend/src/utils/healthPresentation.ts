import type { ServiceStatus } from '../api/health';

export type HealthSummary = {
  healthy: number;
  unhealthy: number;
  checked: number;
  disabled: number;
  allHealthy: boolean;
};

export function summarizeServices(services: Pick<ServiceStatus, 'status'>[]): HealthSummary {
  const healthy = services.filter((service) => service.status === 'healthy').length;
  const unhealthy = services.filter((service) => service.status === 'unhealthy').length;
  const disabled = services.filter(
    (service) => service.status === 'disabled' || service.status === 'not_configured',
  ).length;
  const checked = healthy + unhealthy;

  return {
    healthy,
    unhealthy,
    checked,
    disabled,
    allHealthy: checked > 0 && unhealthy === 0,
  };
}

export function serviceDisplayName(name: string): string {
  return SERVICE_DISPLAY_NAMES[name] ?? name;
}

export function serviceStatusLabel(status: ServiceStatus['status']): string {
  switch (status) {
    case 'healthy': return 'Healthy';
    case 'unhealthy': return 'Unhealthy';
    case 'disabled': return 'Disabled';
    case 'not_configured': return 'Not configured';
  }
}

export function pipelineActivity(pending: number): { label: string; message: string } {
  if (pending === 0) {
    return { label: 'Idle', message: 'No images pending classification' };
  }

  return {
    label: 'Work pending',
    message: `${pending.toLocaleString()} image${pending === 1 ? '' : 's'} pending classification`,
  };
}

const SERVICE_DISPLAY_NAMES: Record<string, string> = {
  postgres: 'PostgreSQL',
  redis: 'Redis',
  minio: 'Object storage',
  api: 'API',
  frontend: 'Frontend',
  ingestion: 'Ingestion worker',
  detection: 'Detection worker',
  classification: 'Classification worker',
  notifications: 'Notifications worker',
  'notifications-email': 'Email notifications worker',
  'notifications-telegram': 'Telegram notifications worker',
  'notifications-earthranger': 'EarthRanger notifications worker',
  'notifications-sensingclues': 'Sensing Clues notifications worker',
  'bulk-upload': 'Bulk-upload worker',
  'cold-tier-watchdog': 'Cold-tier watchdog',
  backup: 'Backup',
};

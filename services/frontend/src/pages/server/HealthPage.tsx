/**
 * System health monitoring page
 *
 * Displays status of all system services for server admins
 */
import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Activity, RefreshCw, CheckCircle2, XCircle, CircleOff, CircleHelp, Loader2 } from 'lucide-react';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../../components/ui/Card';
import { Button } from '../../components/ui/Button';
import { ServerPageLayout } from '../../components/layout/ServerPageLayout';
import { getServicesHealth, type ServiceStatus } from '../../api/health';
import { statisticsApi } from '../../api/statistics';
import { pipelineActivity, serviceDisplayName, serviceStatusLabel, summarizeServices } from '../../utils/healthPresentation';

const ServiceStatusBadge: React.FC<{ status: ServiceStatus }> = ({ status }) => {
  const isHealthy = status.status === 'healthy';
  const isUnhealthy = status.status === 'unhealthy';
  const cardTone = isHealthy
    ? 'bg-green-50 border-green-200'
    : isUnhealthy
      ? 'bg-red-50 border-red-200'
      : 'bg-slate-50 border-slate-200';
  const badgeTone = isHealthy
    ? 'bg-green-100 text-green-700'
    : isUnhealthy
      ? 'bg-red-100 text-red-700'
      : 'bg-slate-100 text-slate-700';

  return (
    <div className={`flex items-start gap-3 p-4 rounded-lg border ${cardTone}`}>
      {isHealthy ? (
        <CheckCircle2 className="h-5 w-5 text-green-600 flex-shrink-0 mt-0.5" />
      ) : isUnhealthy ? (
        <XCircle className="h-5 w-5 text-red-600 flex-shrink-0 mt-0.5" />
      ) : status.status === 'disabled' ? (
        <CircleOff className="h-5 w-5 text-slate-500 flex-shrink-0 mt-0.5" />
      ) : (
        <CircleHelp className="h-5 w-5 text-slate-500 flex-shrink-0 mt-0.5" />
      )}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-medium text-sm">
            {serviceDisplayName(status.name)}
          </span>
          <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${badgeTone}`}>
            {serviceStatusLabel(status.status)}
          </span>
          {isHealthy && status.device && (
            <span className="text-xs px-2 py-0.5 rounded-full font-medium bg-slate-100 text-slate-700">
              {status.device === 'cuda' ? 'GPU' : 'CPU'}
            </span>
          )}
        </div>
        <p className="text-sm text-muted-foreground mt-1 break-words">
          {status.message}
        </p>
      </div>
    </div>
  );
};

export const HealthPage: React.FC = () => {
  // Fetch services health
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['services-health'],
    queryFn: getServicesHealth,
    refetchOnWindowFocus: false,
    retry: false,
  });

  // Fetch pipeline status
  const {
    data: pipelineData,
    isLoading: pipelineLoading,
    error: pipelineError,
    refetch: refetchPipeline,
  } = useQuery({
    queryKey: ['statistics', 'pipeline-status'],
    queryFn: () => statisticsApi.getPipelineStatus(),
  });

  const handleRefresh = () => {
    refetch();
    refetchPipeline();
  };

  const allServices = data?.services ?? [];
  const summary = summarizeServices(allServices);

  return (
    <ServerPageLayout
      title="System health"
      description="Monitor the status of all system services"
    >
      <div className="space-y-6">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Activity className="h-5 w-5 text-muted-foreground" />
                <CardTitle>System services</CardTitle>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={handleRefresh}
                disabled={isLoading}
              >
                <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
            {data && (
              <CardDescription>
                {summary.unhealthy > 0 ? (
                  <span className="text-red-600 font-medium">
                    {summary.healthy} of {summary.checked} checked services are healthy
                    {summary.disabled > 0 && `; ${summary.disabled} disabled or not configured`}
                  </span>
                ) : summary.allHealthy ? (
                  <span className="text-green-600 font-medium">
                    All checked services are healthy ({summary.healthy}/{summary.checked})
                    {summary.disabled > 0 && `; ${summary.disabled} disabled or not configured`}
                  </span>
                ) : summary.disabled > 0 ? (
                  <span className="text-slate-600 font-medium">
                    No configured services are being checked; {summary.disabled} disabled or not configured
                  </span>
                ) : (
                  <span className="text-slate-600 font-medium">
                    No service checks are available
                  </span>
                )}
              </CardDescription>
            )}
          </CardHeader>
          <CardContent>
            {/* Loading State */}
            {isLoading && (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                <span className="ml-3 text-muted-foreground">Checking service health...</span>
              </div>
            )}

            {/* Error State */}
            {error && (
              <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
                <div className="flex items-center gap-2 mb-2">
                  <XCircle className="h-5 w-5 text-red-600" />
                  <span className="font-medium text-red-900">Failed to check service health</span>
                </div>
                <p className="text-sm text-red-700">
                  {error instanceof Error ? error.message : 'Unknown error occurred'}
                </p>
              </div>
            )}

            {/* Services List */}
            {allServices.length > 0 && (
              <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
                {allServices.map((service) => (
                  <ServiceStatusBadge key={service.name} status={service} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="flex items-center space-x-2">
              <Activity className="h-5 w-5 text-muted-foreground" />
              <CardTitle>Pipeline activity</CardTitle>
            </div>
            <CardDescription>Pending image work is progress information, not a worker liveness check.</CardDescription>
          </CardHeader>
          <CardContent>
            {pipelineLoading && (
              <div className="flex items-center gap-3 p-4 rounded-lg border bg-slate-50 border-slate-200">
                <Loader2 className="h-5 w-5 animate-spin text-slate-500" />
                <span className="text-sm text-muted-foreground">Loading pipeline activity...</span>
              </div>
            )}
            {pipelineError && (
              <div className="p-4 bg-red-50 border border-red-200 rounded-lg" role="alert">
                <div className="flex items-center gap-2 mb-2">
                  <XCircle className="h-5 w-5 text-red-600" />
                  <span className="font-medium text-red-900">Failed to load pipeline activity</span>
                </div>
                <p className="text-sm text-red-700">
                  {pipelineError instanceof Error ? pipelineError.message : 'Unknown error occurred'}
                </p>
              </div>
            )}
            {pipelineData && (() => {
              const activity = pipelineActivity(pipelineData.pending);
              return (
                <div className="flex items-start gap-3 p-4 rounded-lg border bg-slate-50 border-slate-200">
                  <Activity className="h-5 w-5 text-slate-500 flex-shrink-0 mt-0.5" />
                  <div>
                    <span className="font-medium text-sm">{activity.label}</span>
                    <p className="text-sm text-muted-foreground mt-1">{activity.message}</p>
                  </div>
                </div>
              );
            })()}
          </CardContent>
        </Card>

      </div>
    </ServerPageLayout>
  );
};

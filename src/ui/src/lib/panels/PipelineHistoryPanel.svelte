<script lang="ts">
  import { onMount } from 'svelte';
  import {
    Clock,
    AlertCircle,
    CheckCircle,
    Loader2,
    History,
  } from 'lucide-svelte';
  import { t } from '../i18n';
  import { api } from '../api';
  import type { Pipeline, TaskStatus } from '../api';

  type PipelineWithId = Pipeline & { id: string };

  import {
    Container,
    List,
    ListHeader,
    ListItem,
    MainPanel,
    Unselected,
  } from '../components/entity';
  import { RefreshButton } from '../components/common';

  let loading = $state(false);
  let error = $state<string | null>(null);
  let pipelines = $state<PipelineWithId[]>([]);
  let selectedPipeline = $state<PipelineWithId | null>(null);
  let selectedId = $state<string | null>(null);

  onMount(() => {
    loadPipelines();
  });

  async function loadPipelines() {
    try {
      loading = true;
      error = null;
      const response = await api.pipeline.listPipelines();
      pipelines = response
        .filter(
          (pipeline): pipeline is NonNullable<typeof pipeline> =>
            pipeline !== null,
        )
        .map((pipeline, index) => ({
          ...pipeline,
          id: pipeline.started_at || `pipeline-${index}`,
          phase: pipeline.phase || 'unknown',
        }));
    } catch (e) {
      error =
        e instanceof Error ? e.message : 'Failed to load pipeline history';
    } finally {
      loading = false;
    }
  }

  function selectPipeline(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    selectedPipeline = pipelines.find((p) => p.id === id) || null;
  }

  function getStatusIcon(status: string) {
    switch (status.toLowerCase()) {
      case 'running':
        return Loader2;
      case 'completed':
        return CheckCircle;
      case 'failed':
        return AlertCircle;
      default:
        return Clock;
    }
  }

  function formatDuration(
    started: string | null | undefined,
    completed: string | null | undefined,
  ) {
    if (!started) return '-';

    const start = new Date(started);
    const end = completed ? new Date(completed) : new Date();
    const duration = end.getTime() - start.getTime();

    const seconds = Math.floor(duration / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  function getTaskCounts(tasks: Record<string, TaskStatus>) {
    const total = Object.keys(tasks).length;
    const completed = Object.values(tasks).filter(
      (t) => t.phase === 'completed',
    ).length;
    const failed = Object.values(tasks).filter(
      (t) => t.phase === 'failed',
    ).length;
    const running = Object.values(tasks).filter(
      (t) => t.phase === 'running',
    ).length;

    return { total, completed, failed, running };
  }

  function getPipelineDescription(pipeline: Pipeline | null) {
    if (!pipeline) return '';
    const counts = getTaskCounts(pipeline.tasks);
    const duration = formatDuration(pipeline.started_at, pipeline.completed_at);
    return `${counts.completed}/${counts.total} tasks • ${duration}`;
  }
</script>

<Container>
  <List slot="list" {loading}>
    <div class="list-header-custom">
      <h3>{$t('pipeline.title')}</h3>
      <RefreshButton onRefresh={loadPipelines} {loading} />
    </div>

    {#if error}
      <div class="error">
        <AlertCircle size={16} />
        {error}
      </div>
    {/if}

    {#each pipelines as pipeline, index}
      {@const pipelineId = pipeline.started_at || `pipeline-${index}`}
      <ListItem
        id={pipelineId}
        title={pipeline.started_at
          ? new Date(pipeline.started_at).toLocaleString()
          : 'Pipeline'}
        description={getPipelineDescription(pipeline)}
        selected={selectedId === pipelineId}
        onclick={() => selectPipeline(pipelineId)}
      />
    {/each}

    {#if pipelines.length === 0 && !loading}
      <div class="empty-list">
        <History size={48} class="text-gray-400" />
        <h4>{$t('pipeline.empty')}</h4>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if selectedPipeline}
      <div class="pipeline-detail">
        <div class="detail-header">
          <div class="header-top">
            <div class="status-info">
              {#snippet statusIcon()}{@const StatusIcon = getStatusIcon(
                  selectedPipeline!.phase,
                )}<StatusIcon
                  size={20}
                  class="status-{selectedPipeline!.phase.toLowerCase()}"
                />{/snippet}
              {@render statusIcon()}
              <h1>
                {$t(`pipeline.phase.${selectedPipeline!.phase.toLowerCase()}`)}
              </h1>
            </div>
            <RefreshButton onRefresh={loadPipelines} {loading} />
          </div>

          <dl class="timing-info">
            {#if selectedPipeline!.started_at}
              <div class="time-item">
                <dt>{$t('pipeline.detail_labels.started')}</dt>
                <dd>
                  {new Date(selectedPipeline!.started_at).toLocaleString()}
                </dd>
              </div>
            {/if}
            {#if selectedPipeline!.completed_at}
              <div class="time-item">
                <dt>{$t('pipeline.detail_labels.completed')}</dt>
                <dd>
                  {new Date(selectedPipeline!.completed_at).toLocaleString()}
                </dd>
              </div>
            {/if}
            <div class="time-item">
              <dt>{$t('pipeline.detail_labels.duration')}</dt>
              <dd>
                {formatDuration(
                  selectedPipeline!.started_at,
                  selectedPipeline!.completed_at,
                )}
              </dd>
            </div>
          </dl>
        </div>

        <div class="task-section">
          <h3>{$t('pipeline.detail_labels.tasks')}</h3>
          <div class="task-list">
            {#each Object.entries(selectedPipeline!.tasks).sort( ([, a], [, b]) => {
                if (!a.completed_at && !b.completed_at) return 0;
                if (!a.completed_at) return 1;
                if (!b.completed_at) return -1;
                return new Date(a.completed_at).getTime() - new Date(b.completed_at).getTime();
              }, ) as [taskName, task]}
              <div class="task-item">
                <div class="task-header">
                  {#snippet taskIcon()}{@const TaskIcon = getStatusIcon(
                      task.phase,
                    )}<TaskIcon
                      size={16}
                      class="status-{task.phase.toLowerCase()}"
                    />{/snippet}
                  {@render taskIcon()}
                  <h5 class="task-name">{taskName}</h5>
                  <span class="task-status status-{task.phase.toLowerCase()}">
                    {$t(`pipeline.phase.${task.phase.toLowerCase()}`)}
                  </span>
                </div>

                <div class="task-details">
                  {#if task.started_at}
                    <span class="task-time">
                      {$t('pipeline.detail_labels.started_time', {
                        values: {
                          time: new Date(task.started_at).toLocaleTimeString(),
                        },
                      })}
                    </span>
                  {/if}
                  {#if task.completed_at}
                    <span class="task-time">
                      {$t('pipeline.detail_labels.completed_time', {
                        values: {
                          time: new Date(
                            task.completed_at,
                          ).toLocaleTimeString(),
                        },
                      })}
                    </span>
                  {/if}
                  <span class="task-duration">
                    {$t('pipeline.detail_labels.duration_time', {
                      values: {
                        duration: formatDuration(
                          task.started_at,
                          task.completed_at,
                        ),
                      },
                    })}
                  </span>
                </div>

                {#if task.error}
                  <div class="task-error">
                    <AlertCircle size={14} />
                    <span>{task.error.message}</span>
                  </div>
                {/if}
              </div>
            {/each}
          </div>
        </div>
      </div>
    {:else}
      <Unselected />
    {/if}
  </MainPanel>
</Container>

<style>
  .error {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem 1rem;
    margin: 0 1rem;
    background-color: var(--color-error-bg);
    border: 1px solid var(--color-error-border);
    border-radius: 0.375rem;
    color: var(--color-error);
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
  }

  .empty-list {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 3rem 1rem;
    gap: 1rem;
    text-align: center;
    color: var(--color-text-light);
  }

  .empty-list h4 {
    margin: 0;
    font-size: var(--font-h4-size);
    font-weight: var(--font-h4-weight);
    line-height: var(--font-h4-line-height);
  }

  .list-header-custom {
    padding: 16px;
    border-bottom: 1px solid var(--color-border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: white;
  }

  .list-header-custom h3 {
    margin: 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
    color: var(--color-text-primary);
  }

  .pipeline-detail {
    padding: 1.5rem;
    height: 100%;
    overflow-y: auto;
  }

  .detail-header {
    margin-bottom: 2rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid #e2e8f0;
  }

  .header-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1rem;
  }

  .status-info {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  .status-info h1 {
    margin: 0;
    font-size: var(--font-h1-size);
    font-weight: var(--font-h1-weight);
    line-height: var(--font-h1-line-height);
    color: var(--color-text-dark);
  }

  .timing-info {
    display: flex;
    flex-wrap: wrap;
    gap: 1.5rem;
  }

  .time-item {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .time-item dt {
    font-size: var(--font-caption-size);
    font-weight: var(--font-label-weight);
    color: var(--color-text-gray);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .time-item dd {
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
    color: var(--color-text-dark);
  }

  .task-section {
    margin-top: 1.5rem;
  }

  .task-section h3 {
    margin: 0 0 1rem 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
    color: var(--color-text-dark);
  }

  .task-list {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .task-item {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 0.5rem;
    padding: 1rem;
  }

  .task-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }

  .task-name {
    font-size: var(--font-h5-size);
    font-weight: var(--font-h5-weight);
    line-height: var(--font-h5-line-height);
    color: var(--color-text-dark);
    flex: 1;
    margin: 0;
  }

  .task-status {
    padding: 0.25rem 0.75rem;
    border-radius: 0.25rem;
    font-size: var(--font-caption-size);
    font-weight: var(--font-label-weight);
    text-transform: uppercase;
  }

  .task-details {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    margin-bottom: 0.5rem;
  }

  .task-time,
  .task-duration {
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    color: var(--color-text-gray);
  }

  .task-error {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem;
    background: #fee2e2;
    border: 1px solid #fecaca;
    border-radius: 0.25rem;
    color: var(--color-error);
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
    margin-top: 0.5rem;
  }

  :global(.status-completed) {
    color: #16a34a;
  }

  :global(.status-failed) {
    color: var(--color-error);
  }

  :global(.status-running) {
    color: #ea580c;
  }

  :global(.status-waiting) {
    color: var(--color-text-light);
  }

  .status-completed {
    background: #dcfce7;
    color: #16a34a;
  }

  .status-failed {
    background: #fee2e2;
    color: var(--color-error);
  }

  .status-running {
    background: #fed7aa;
    color: #ea580c;
  }

  .status-waiting {
    background: #f3f4f6;
    color: var(--color-text-light);
  }

  :global(.animate-spin) {
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }
</style>

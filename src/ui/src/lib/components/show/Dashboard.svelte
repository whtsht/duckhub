<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { DashboardConfig } from '../../api';
  import ShowPanel from '../entity/ShowPanel.svelte';
  import { t } from '../../i18n';
  import ChartComponent from '../../ChartComponent.svelte';
  import { api } from '../../api';

  const dispatch = createEventDispatcher();

  let { entity }: { entity: { name: string; config: DashboardConfig } | null } =
    $props();

  let loading = $state(true);
  let dashboardData = $state<any>(null);

  $effect(() => {
    if (entity) {
      loadDashboardData();
    }
  });

  async function loadDashboardData() {
    if (!entity) return;

    loading = true;
    try {
      dashboardData = await api.dashboards.getData(entity.name);
    } catch (err) {
    } finally {
      loading = false;
    }
  }

  function handleEdit() {
    dispatch('edit');
  }

  function handleDelete() {
    dispatch('delete');
  }
</script>

{#if entity}
  <ShowPanel title={entity.name} {handleEdit} {handleDelete}>
    {#if entity.config.description}
      <div class="section">
        <h4>{$t('dashboards.detail.description')}</h4>
        <p class="description">{entity.config.description}</p>
      </div>
    {/if}

    <div class="chart-container">
      {#if loading}
        <div class="loading-state">
          <div class="spinner"></div>
          <p>{$t('dashboards.loading_chart')}</p>
        </div>
      {:else if dashboardData && entity}
        <ChartComponent
          chartType={entity.config.chart.type}
          labels={dashboardData.labels.map((l: any) => String(l))}
          values={dashboardData.values.map((v: any) => Number(v))}
          xAxisLabel={entity.config.chart.x_column}
          yAxisLabel={entity.config.chart.y_column}
        />
      {:else}
        <div class="empty-chart">
          <p>{$t('dashboards.no_data')}</p>
        </div>
      {/if}
    </div>
  </ShowPanel>
{/if}

<style>
  .section {
    margin-bottom: 32px;
  }

  .section h4 {
    margin: 0 0 16px 0;
    font-size: var(--font-h4-size);
    font-weight: var(--font-h4-weight);
    line-height: var(--font-h4-line-height);
    color: var(--color-text-primary);
  }

  .description {
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-secondary);
    margin: 0;
  }

  .chart-container {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 400px;
  }

  .loading-state,
  .empty-chart {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    color: var(--color-text-gray);
  }

  .spinner {
    width: 32px;
    height: 32px;
    border: 3px solid var(--color-border-subtle);
    border-top: 3px solid var(--color-info);
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }
</style>

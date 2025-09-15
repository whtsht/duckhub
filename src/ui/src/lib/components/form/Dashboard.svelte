<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import type { DashboardConfig } from '../../api';
  import FormPanel from '../entity/FormPanel.svelte';
  import { t } from '../../i18n';
  import { Button } from '../common';
  import { Save } from 'lucide-svelte';
  import { api } from '../../api';

  const dispatch = createEventDispatcher();

  let {
    mode = 'create',
    initialData = null,
  }: {
    mode?: 'create' | 'edit';
    initialData?: any | null;
  } = $props();

  let queries = $state<any[]>([]);

  onMount(async () => {
    queries = await api.queries.list();
  });

  let name = $state('');
  let description = $state('');
  let queryName = $state('');
  let chartType = $state<'line' | 'bar'>('line');
  let xColumn = $state('');
  let yColumn = $state('');

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name || '';
      description = initialData.description || '';
      queryName = initialData.query || '';
      chartType = initialData.chart?.type || 'line';
      xColumn = initialData.chart?.x_column || '';
      yColumn = initialData.chart?.y_column || '';
    } else if (!initialData && mode === 'create') {
      name = '';
      description = '';
      queryName = '';
      chartType = 'line';
      xColumn = '';
      yColumn = '';
    }
  });

  function handleSubmit() {
    if (!name || !queryName || !xColumn || !yColumn) {
      return;
    }

    const dashboardData = {
      name: name,
      config: {
        description: description || undefined,
        query: queryName,
        chart: {
          type: chartType,
          x_column: xColumn,
          y_column: yColumn,
        },
      },
    };

    dispatch('submit', dashboardData);
  }

  function handleCancel() {
    dispatch('close');
  }

  function isFormValid() {
    return name && queryName && xColumn && yColumn;
  }
</script>

<FormPanel {handleSubmit} {handleCancel}>
  <div class="form-group">
    <label for="name">{$t('dashboards.form.name')}</label>
    <input
      id="name"
      type="text"
      bind:value={name}
      disabled={mode === 'edit'}
      placeholder={$t('dashboards.form.name_placeholder')}
    />
  </div>

  <div class="form-group">
    <label for="description">{$t('dashboards.form.description')}</label>
    <textarea
      id="description"
      bind:value={description}
      placeholder={$t('dashboards.form.description_placeholder')}
      rows="3"
    ></textarea>
  </div>

  <div class="form-group">
    <label for="query">{$t('dashboards.form.query')}</label>
    <select id="query" bind:value={queryName}>
      <option value="">{$t('dashboards.form.select_query')}</option>
      {#each queries as query}
        <option value={query.name}>{query.name}</option>
      {/each}
    </select>
  </div>

  <div class="form-group">
    <label for="chart-type">{$t('dashboards.form.chart_type')}</label>
    <select id="chart-type" bind:value={chartType}>
      <option value="line">{$t('dashboards.chart_types.line')}</option>
      <option value="bar">{$t('dashboards.chart_types.bar')}</option>
    </select>
  </div>

  <div class="form-row">
    <div class="form-group">
      <label for="x-column">{$t('dashboards.form.x_column')}</label>
      <input
        id="x-column"
        type="text"
        bind:value={xColumn}
        placeholder={$t('dashboards.form.x_column_placeholder')}
      />
    </div>

    <div class="form-group">
      <label for="y-column">{$t('dashboards.form.y_column')}</label>
      <input
        id="y-column"
        type="text"
        bind:value={yColumn}
        placeholder={$t('dashboards.form.y_column_placeholder')}
      />
    </div>
  </div>
  {#snippet actions()}
    <Button icon={Save} onclick={handleSubmit} disabled={!isFormValid()}>
      {$t(mode === 'create' ? 'common.create' : 'common.update')}
    </Button>
  {/snippet}
</FormPanel>

<style>
  .form-group {
    margin-bottom: 20px;
  }

  .form-group label {
    display: block;
    margin-bottom: 6px;
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--color-text-primary);
  }

  .form-group input,
  .form-group textarea,
  .form-group select {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    font-size: 0.95rem;
    font-family: inherit;
  }

  .form-group input:focus,
  .form-group textarea:focus,
  .form-group select:focus {
    outline: none;
    border-color: var(--color-primary);
  }

  .form-group input:disabled {
    background: var(--color-background-disabled);
    cursor: not-allowed;
  }

  .form-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }
</style>

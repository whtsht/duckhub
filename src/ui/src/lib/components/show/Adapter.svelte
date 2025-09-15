<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { AdapterConfig } from '../../api';
  import ShowPanel from '../entity/ShowPanel.svelte';
  import { t } from '../../i18n';

  const dispatch = createEventDispatcher();

  let { entity }: { entity: { name: string; config: AdapterConfig } | null } =
    $props();

  function handleEdit() {
    dispatch('edit');
  }

  function handleDelete() {
    dispatch('delete');
  }
</script>

{#if entity}
  <ShowPanel title={entity.name} {handleEdit} {handleDelete}>
    <div class="section">
      <h4>{$t('adapters.detail.description')}</h4>
      <p class="description">{entity.config.description}</p>
    </div>

    <div class="section">
      <h4>{$t('adapters.detail.data_source')}</h4>
      {#if entity.config.source.type === 'file'}
        <dl class="info-grid">
          <div class="info-item">
            <dt>{$t('adapters.detail.file_path')}</dt>
            <dd class="path">{entity.config.source.file?.path}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('adapters.detail.format')}</dt>
            <dd class="format">{entity.config.source.format?.type}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('adapters.detail.connection_name')}</dt>
            <dd>{entity.config.connection}</dd>
          </div>
          {#if entity.config.source.file?.compression}
            <div class="info-item">
              <dt>{$t('adapters.detail.compression')}</dt>
              <dd>{entity.config.source.file.compression}</dd>
            </div>
          {/if}
          {#if entity.config.source.format?.delimiter}
            <div class="info-item">
              <dt>{$t('adapters.detail.delimiter')}</dt>
              <dd>"{entity.config.source.format.delimiter}"</dd>
            </div>
          {/if}
          {#if entity.config.source.format?.has_header !== null && entity.config.source.format?.has_header !== undefined}
            <div class="info-item">
              <dt>{$t('adapters.detail.header_row')}</dt>
              <dd>
                {entity.config.source.format.has_header
                  ? $t('adapters.detail.has_header')
                  : $t('adapters.detail.no_header')}
              </dd>
            </div>
          {/if}
        </dl>
      {:else if entity.config.source.type === 'database'}
        <dl class="info-grid">
          <div class="info-item">
            <dt>{$t('adapters.detail.table_name')}</dt>
            <dd class="table-name">{entity.config.source.table_name}</dd>
          </div>
        </dl>
      {/if}
    </div>

    <div class="section">
      <h4>{$t('adapters.detail.columns')}</h4>
      <div class="columns-table">
        <div class="table-header">
          <span>{$t('adapters.detail.column_name')}</span>
          <span>{$t('adapters.detail.column_type')}</span>
          <span>{$t('adapters.detail.column_description')}</span>
        </div>
        {#each entity.config.columns as column}
          <div class="table-row">
            <span class="column-name">{column.name}</span>
            <span class="column-type">{column.type}</span>
            <span class="column-description">{column.description || '-'}</span>
          </div>
        {/each}
      </div>
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

  .info-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }

  .info-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .info-item dt {
    font-size: var(--font-label-size);
    font-weight: var(--font-label-weight);
    line-height: var(--font-label-line-height);
    color: var(--color-text-secondary);
  }

  .info-item dd {
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-primary);
    word-break: break-all;
  }

  .path {
    font-family: monospace;
    background: var(--color-background-hover);
    padding: 4px 8px;
    border-radius: 4px;
    font-size: var(--font-small-size);
  }

  .format,
  .table-name {
    display: inline-block;
    background: var(--color-background-selected);
    color: var(--color-primary);
    padding: 2px 8px;
    border-radius: 3px;
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
  }

  .columns-table {
    border: 1px solid var(--color-border);
    border-radius: 4px;
    overflow: hidden;
  }

  .table-header {
    display: grid;
    grid-template-columns: 1fr 120px 2fr;
    background: var(--color-background-hover);
    padding: 12px 16px;
    font-weight: var(--font-label-weight);
    font-size: var(--font-small-size);
    color: var(--color-text-secondary);
    border-bottom: 1px solid #e0e0e0;
  }

  .table-row {
    display: grid;
    grid-template-columns: 1fr 120px 2fr;
    padding: 12px 16px;
    border-bottom: 1px solid var(--color-border-light);
  }

  .table-row:last-child {
    border-bottom: none;
  }

  .column-name {
    font-family: monospace;
    font-weight: var(--font-label-weight);
    color: var(--color-text-primary);
  }

  .column-type {
    color: #9b59b6;
    font-weight: var(--font-label-weight);
    font-size: var(--font-small-size);
  }

  .column-description {
    color: var(--color-text-secondary);
    font-size: var(--font-small-size);
  }
</style>

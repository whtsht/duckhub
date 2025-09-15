<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ConnectionConfig } from '../../api';
  import ShowPanel from '../entity/ShowPanel.svelte';
  import { t } from '../../i18n';
  import { ConnectionTestButton } from '../common';

  const dispatch = createEventDispatcher();

  let {
    entity,
  }: { entity: { name: string; config: ConnectionConfig } | null } = $props();

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
      <h3>{$t('connections.detail.connection_info')}</h3>
      <dl class="info-grid">
        <div class="info-item">
          <dt>{$t('connections.detail.type')}</dt>
          <dd class="connection-type">{entity.config.type}</dd>
        </div>

        {#if entity.config.type === 'sqlite'}
          <div class="info-item full-width">
            <dt>{$t('connections.detail.database_path')}</dt>
            <dd class="path">{entity.config.config.path}</dd>
          </div>
        {:else if entity.config.type === 'localfile'}
          <div class="info-item full-width">
            <dt>{$t('connections.detail.base_path')}</dt>
            <dd class="path">{entity.config.config.base_path}</dd>
          </div>
        {:else if entity.config.type === 'mysql' || entity.config.type === 'postgresql'}
          <div class="info-item">
            <dt>{$t('connections.detail.host')}</dt>
            <dd>{entity.config.config.host}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('connections.detail.port')}</dt>
            <dd>{entity.config.config.port}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('connections.detail.database')}</dt>
            <dd>{entity.config.config.database}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('connections.detail.username')}</dt>
            <dd>{entity.config.config.username}</dd>
          </div>
        {:else if entity.config.type === 's3'}
          <div class="info-item">
            <dt>{$t('connections.detail.bucket')}</dt>
            <dd>{entity.config.config.bucket}</dd>
          </div>
          <div class="info-item">
            <dt>{$t('connections.detail.region')}</dt>
            <dd>
              {entity.config.config.region || $t('connections.detail.default')}
            </dd>
          </div>
          {#if entity.config.config.endpoint_url}
            <div class="info-item full-width">
              <dt>{$t('connections.detail.endpoint_url')}</dt>
              <dd class="path">{entity.config.config.endpoint_url}</dd>
            </div>
          {/if}
        {/if}
      </dl>
    </div>

    <div class="section">
      <h3>{$t('connections.detail.connection_test')}</h3>
      <div class="test-info">
        <ConnectionTestButton connectionConfig={entity.config} />
      </div>
    </div>
  </ShowPanel>
{/if}

<style>
  .section {
    margin-bottom: 32px;
  }

  .section h3 {
    margin: 0 0 16px 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
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

  .info-item.full-width {
    grid-column: 1 / -1;
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
  }

  .connection-type {
    display: inline-block;
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    text-transform: uppercase;
    color: var(--color-text-primary);
  }

  .path {
    font-family: monospace;
    background: var(--color-background-hover);
    padding: 4px 8px;
    border-radius: 4px;
    font-size: var(--font-small-size);
  }

  .test-info {
    background: var(--color-background-hover);
    padding: 16px;
    border-radius: 4px;
  }
</style>

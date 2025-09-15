<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ModelConfig } from '../../api';
  import ShowPanel from '../entity/ShowPanel.svelte';
  import { t } from '../../i18n';
  import { Code } from 'lucide-svelte';

  const dispatch = createEventDispatcher();

  let { entity }: { entity: { name: string; config: ModelConfig } | null } =
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
    {#if entity.config.description}
      <div class="section">
        <h4>{$t('models.detail.description')}</h4>
        <p class="description">{entity.config.description}</p>
      </div>
    {/if}

    <div class="section">
      <h4>
        <Code size={18} />
        {$t('models.detail.sql_query')}
      </h4>
      <div class="sql-code">
        <pre><code>{entity.config.sql}</code></pre>
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
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .description {
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-secondary);
    margin: 0;
  }

  .sql-code {
    background: var(--color-background-code);
    border: 1px solid var(--color-border);
    border-radius: 4px;
    overflow-x: auto;
  }

  .sql-code pre {
    margin: 0;
    padding: 16px;
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: var(--font-small-size);
    line-height: var(--font-small-line-height);
    color: var(--color-text-primary);
  }
</style>

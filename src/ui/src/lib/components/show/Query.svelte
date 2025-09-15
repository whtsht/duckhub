<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { ShowPanel } from '../entity';
  import SqlEditor from '../SqlEditor.svelte';
  import { t } from '../../i18n';

  let {
    entity,
    loading = false,
  }: {
    entity: any;
    loading?: boolean;
  } = $props();

  const dispatch = createEventDispatcher();

  function handleEdit() {
    dispatch('edit', entity);
  }

  function handleDelete() {
    if (
      confirm($t('query.delete_confirm', { values: { name: entity?.name } }))
    ) {
      dispatch('delete', entity);
    }
  }
</script>

{#if entity}
  <ShowPanel title={entity.name} {handleEdit} {handleDelete} {loading}>
    <div class="query-content">
      {#if entity.description}
        <div class="query-description">
          <h4>{$t('query.detail.description')}</h4>
          <p>{entity.description}</p>
        </div>
      {/if}

      <div class="sql-editor-section">
        <h4>{$t('query.detail.sql_query')}</h4>
        <SqlEditor
          sql={entity.sql}
          readonly={true}
          enableExecution={true}
          context="query"
        />
      </div>
    </div>
  </ShowPanel>
{/if}

<style>
  .query-content {
    display: flex;
    flex-direction: column;
    gap: 24px;
    height: 100%;
  }

  .query-description h4,
  .sql-editor-section h4 {
    margin: 0 0 12px 0;
    font-size: var(--font-h4-size);
    font-weight: var(--font-h4-weight);
    line-height: var(--font-h4-line-height);
    color: var(--color-text-slate);
  }

  .query-description p {
    margin: 0;
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-gray);
  }

  .sql-editor-section {
    flex: 1;
    display: flex;
    flex-direction: column;
  }
</style>

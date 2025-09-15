<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { FormPanel } from '../entity';
  import SqlEditor from '../SqlEditor.svelte';
  import { Button } from '../common';
  import { Save } from 'lucide-svelte';
  import { t } from '../../i18n';

  let {
    mode = 'create',
    initialData = null,
    loading = false,
  }: {
    mode?: 'create' | 'edit';
    initialData?: any | null;
    loading?: boolean;
  } = $props();

  const dispatch = createEventDispatcher();

  let name = $state('');
  let description = $state('');
  let sql = $state('');

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name || '';
      description = initialData.description || '';
      sql = initialData.sql || '';
    }
  });

  function handleSubmit() {
    const data = {
      name: name.trim(),
      description: description.trim() || undefined,
      sql: sql.trim(),
    };
    dispatch('submit', data);
  }

  function handleCancel() {
    name = '';
    description = '';
    sql = '';
    dispatch('close');
  }

  function isFormValid() {
    return name.trim() && sql.trim();
  }
</script>

<FormPanel {handleSubmit} {handleCancel}>
  <div class="form-group">
    <label for="name">{$t('query.name_label')}</label>
    <input
      id="name"
      type="text"
      bind:value={name}
      disabled={mode === 'edit'}
      placeholder={$t('query.name_placeholder')}
    />
  </div>

  <div class="form-group">
    <label for="description">{$t('query.description_label')}</label>
    <input
      id="description"
      type="text"
      bind:value={description}
      placeholder={$t('query.description_placeholder')}
    />
  </div>

  <div class="form-group">
    <SqlEditor
      bind:sql
      placeholder={$t('query.sql_placeholder')}
      rows={12}
      enableExecution={true}
      context="query"
    />
  </div>

  {#snippet actions()}
    <Button icon={Save} onclick={handleSubmit} disabled={!isFormValid()}>
      {$t(mode === 'create' ? 'common.create' : 'common.update')}
    </Button>
  {/snippet}
</FormPanel>

<style>
  .form-group {
    margin-bottom: 24px;
  }

  .form-group label {
    display: block;
    margin-bottom: 8px;
    font-size: 0.875rem;
    font-weight: 600;
    color: var(--color-text-slate);
  }

  .form-group input {
    width: 100%;
    padding: 12px;
    border: 1px solid var(--color-border-lighter);
    border-radius: 6px;
    font-size: 0.875rem;
    font-family: inherit;
    background: white;
    transition: border-color 0.2s;
  }

  .form-group input:focus {
    outline: none;
    border-color: var(--color-success);
    box-shadow: 0 0 0 3px rgba(5, 150, 105, 0.1);
  }

  .form-group input:disabled {
    background: var(--color-background-light);
    color: var(--color-text-light);
    cursor: not-allowed;
  }
</style>

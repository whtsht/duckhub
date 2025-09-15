<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ModelConfig } from '../../api';
  import { t } from '../../i18n';
  import { Button } from '../common';
  import SqlEditor from '../SqlEditor.svelte';
  import { Save } from 'lucide-svelte';
  import FormPanel from '../entity/FormPanel.svelte';

  const dispatch = createEventDispatcher();

  let {
    mode = 'create',
    initialData = null,
    selectedId = '',
  }: {
    mode?: 'create' | 'edit';
    initialData?: ModelConfig | null;
    selectedId?: string;
  } = $props();

  let name = $state('');
  let description = $state('');
  let sql = $state('');

  $effect(() => {
    if (initialData && mode === 'edit') {
      description = initialData.description || '';
      sql = initialData.sql || '';
    } else if (!initialData && mode === 'create') {
      name = '';
      description = '';
      sql = '';
    }
  });

  function handleSubmit() {
    const config: any = {
      sql: sql.trim(),
    };

    if (description.trim()) {
      config.description = description.trim();
    }

    const modelName = mode === 'create' ? name.trim() : selectedId || '';

    dispatch('submit', {
      name: modelName,
      config,
    });
  }

  function handleCancel() {
    name = '';
    description = '';
    sql = '';
    dispatch('close');
  }

  function isFormValid() {
    if (mode === 'create') {
      return name.trim() && sql.trim();
    } else {
      return sql.trim();
    }
  }
</script>

<FormPanel {handleSubmit} {handleCancel}>
  {#if mode === 'create'}
    <div class="form-group">
      <label for="name">{$t('models.form.name_label')}</label>
      <input
        id="name"
        type="text"
        bind:value={name}
        placeholder={$t('models.form.name_placeholder')}
      />
    </div>
  {/if}

  <div class="form-group">
    <label for="description">{$t('models.form.description_label')}</label>
    <textarea
      id="description"
      bind:value={description}
      placeholder={$t('models.form.description_placeholder')}
      rows="2"
    ></textarea>
  </div>

  <div class="form-group">
    <SqlEditor
      bind:sql
      placeholder={$t('models.form.sql_placeholder')}
      rows={10}
      enableExecution={true}
      context="model"
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
  .form-group textarea {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    font-size: 0.95rem;
    font-family: inherit;
  }

  .form-group input:focus,
  .form-group textarea:focus {
    outline: none;
    border-color: var(--color-primary);
  }
</style>

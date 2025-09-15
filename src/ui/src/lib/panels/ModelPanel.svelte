<script lang="ts">
  import { onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { t } from '../i18n';
  import { api } from '../api';
  import type { ModelSummary, ModelConfig } from '../api';
  import {
    showFormSuccessToast,
    showDeleteSuccessToast,
    showFormErrorToast,
  } from '../utils/formHandlers';

  import {
    Container,
    List,
    ListHeader,
    ListItem,
    MainPanel,
    Unselected,
  } from '../components/entity';

  import ModelDetail from '../components/show/Model.svelte';
  import ModelForm from '../components/form/Model.svelte';

  let models: ModelSummary[] = $state([]);
  let selectedModel: ModelConfig | null = $state(null);
  let selectedId: string | null = $state(null);
  let viewMode: 'detail' | 'form' = $state('detail');
  let formMode: 'create' | 'edit' = $state('create');
  let loading = $state(false);

  async function loadModels() {
    loading = true;
    models = await api.models.list();
    loading = false;
  }

  async function selectModel(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    viewMode = 'detail';
    selectedModel = await api.models.get(id);
  }

  function createNew() {
    selectedModel = null;
    selectedId = null;
    viewMode = 'form';
    formMode = 'create';
  }

  function editModel() {
    viewMode = 'form';
    formMode = 'edit';
  }

  async function deleteModel() {
    if (!selectedId) return;

    const confirmMessage = get(t)('models.delete.confirm');

    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      await api.models.delete(selectedId);
      showDeleteSuccessToast('models', selectedId);
      await loadModels();

      selectedModel = null;
      selectedId = null;
      viewMode = 'detail';
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'models',
      );
    }
  }

  async function saveModel(event: CustomEvent) {
    const data = event.detail;

    try {
      if (formMode === 'create') {
        await api.models.create({
          name: data.name,
          config: data.config,
        });
        showFormSuccessToast({
          entityType: 'models',
          mode: 'create',
          entityName: data.name,
        });
      } else if (selectedId) {
        await api.models.update(selectedId, data.config);
        showFormSuccessToast({
          entityType: 'models',
          mode: 'edit',
          entityName: selectedId,
        });
      }

      await loadModels();

      if (formMode === 'create' && data.name) {
        await selectModel(data.name);
      } else {
        viewMode = 'detail';
        if (selectedId) {
          selectedModel = await api.models.get(selectedId);
        }
      }
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'models',
      );
    }
  }

  onMount(() => {
    loadModels();
  });
</script>

<Container>
  <List slot="list" {loading}>
    <ListHeader title={$t('models.title')} onCreate={createNew}>
      {#snippet buttonText()}
        {$t('models.create')}
      {/snippet}
    </ListHeader>

    {#each models as model}
      <ListItem
        id={model.name}
        title={model.name}
        description={model.description}
        selected={selectedId === model.name}
        onclick={() => selectModel(model.name)}
      />
    {/each}

    {#if models.length === 0 && !loading}
      <div class="empty-list">
        <p>{$t('models.empty.message')}</p>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if viewMode === 'detail' && selectedModel}
      <ModelDetail
        entity={selectedId && selectedModel
          ? { name: selectedId, config: selectedModel }
          : null}
        on:edit={editModel}
        on:delete={deleteModel}
      />
    {:else if viewMode === 'form'}
      <ModelForm
        mode={formMode}
        initialData={formMode === 'edit' ? selectedModel : null}
        selectedId={selectedId || undefined}
        on:submit={saveModel}
        on:close={() => {
          viewMode = 'detail';
          if (!selectedId) {
            selectedModel = null;
          }
        }}
      />
    {:else}
      <Unselected />
    {/if}
  </MainPanel>
</Container>

<style>
  .empty-list {
    padding: 24px;
    text-align: center;
    color: var(--color-text-muted);
  }

  .empty-list p {
    margin: 0;
  }
</style>

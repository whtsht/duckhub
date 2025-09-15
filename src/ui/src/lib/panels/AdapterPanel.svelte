<script lang="ts">
  import { onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { t } from '../i18n';
  import { api } from '../api';
  import type { AdapterSummary, AdapterConfig } from '../api';
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

  import AdapterDetail from '../components/show/Adapter.svelte';
  import AdapterForm from '../components/form/Adapter.svelte';

  let adapters: AdapterSummary[] = $state([]);
  let selectedAdapter: AdapterConfig | null = $state(null);
  let selectedId: string | null = $state(null);
  let viewMode: 'detail' | 'form' = $state('detail');
  let formMode: 'create' | 'edit' = $state('create');
  let loading = $state(false);

  async function loadAdapters() {
    loading = true;
    try {
      adapters = await api.adapters.list();
    } finally {
      loading = false;
    }
  }

  async function selectAdapter(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    viewMode = 'detail';
    selectedAdapter = await api.adapters.get(id);
  }

  function createNew() {
    selectedAdapter = null;
    selectedId = null;
    viewMode = 'form';
    formMode = 'create';
  }

  function editAdapter() {
    viewMode = 'form';
    formMode = 'edit';
  }

  async function deleteAdapter() {
    if (!selectedId) return;

    const confirmMessage = get(t)('adapters.delete.confirm', {
      values: { name: selectedId },
    });

    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      await api.adapters.delete(selectedId);
      showDeleteSuccessToast('adapters', selectedId);
      await loadAdapters();

      selectedAdapter = null;
      selectedId = null;
      viewMode = 'detail';
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'adapters',
      );
    }
  }

  async function saveAdapter(event: CustomEvent) {
    const data = event.detail;

    try {
      if (formMode === 'create') {
        await api.adapters.create(data);
        showFormSuccessToast({
          entityType: 'adapters',
          mode: 'create',
          entityName: data.name,
        });
      } else if (selectedId) {
        await api.adapters.update(selectedId, data.config);
        showFormSuccessToast({
          entityType: 'adapters',
          mode: 'edit',
          entityName: selectedId,
        });
      }

      await loadAdapters();

      if (formMode === 'create' && data.name) {
        await selectAdapter(data.name);
      } else {
        viewMode = 'detail';
        if (selectedId) {
          selectedAdapter = await api.adapters.get(selectedId);
        }
      }
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'adapters',
      );
    }
  }

  onMount(() => {
    loadAdapters();
  });
</script>

<Container>
  <List slot="list" {loading}>
    <ListHeader title={$t('adapters.title')} onCreate={createNew}>
      {#snippet buttonText()}
        {$t('adapters.create')}
      {/snippet}
    </ListHeader>

    {#each adapters as adapter}
      <ListItem
        id={adapter.name}
        title={adapter.name}
        description={adapter.description}
        selected={selectedId === adapter.name}
        onclick={() => selectAdapter(adapter.name)}
      />
    {/each}

    {#if adapters.length === 0 && !loading}
      <div class="empty-list">
        <p>{$t('adapters.empty.message')}</p>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if viewMode === 'detail' && selectedAdapter}
      <AdapterDetail
        entity={selectedId && selectedAdapter
          ? { name: selectedId, config: selectedAdapter }
          : null}
        on:edit={editAdapter}
        on:delete={deleteAdapter}
      />
    {:else if viewMode === 'form'}
      <AdapterForm
        mode={formMode}
        initialData={formMode === 'edit' && selectedAdapter
          ? { name: selectedId || '', config: selectedAdapter }
          : null}
        on:submit={saveAdapter}
        on:close={() => {
          viewMode = 'detail';
          if (!selectedId) {
            selectedAdapter = null;
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

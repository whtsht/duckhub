<script lang="ts">
  import { onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { t } from '../i18n';
  import { api } from '../api';
  import type {
    ConnectionSummary,
    ConnectionConfig as ApiConnectionConfig,
  } from '../api';
  import type { ConnectionConfig } from '../api';
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

  import ConnectionDetail from '../components/show/Connection.svelte';
  import ConnectionForm from '../components/form/Connection.svelte';

  let connections: ConnectionSummary[] = $state([]);
  type ConnectionWithName = { name: string; config: ConnectionConfig };
  let selectedConnection: ConnectionWithName | null = $state(null);
  let selectedId: string | null = $state(null);
  let viewMode: 'detail' | 'form' = $state('detail');
  let formMode: 'create' | 'edit' = $state('create');
  let loading = $state(false);
  async function loadConnections() {
    loading = true;
    connections = await api.connections.list();
    loading = false;
  }

  async function selectConnection(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    viewMode = 'detail';
    const details = await api.connections.get(id);
    selectedConnection = { name: id, config: details };
  }

  function createNew() {
    selectedConnection = null;
    selectedId = null;
    viewMode = 'form';
    formMode = 'create';
  }

  function editConnection() {
    viewMode = 'form';
    formMode = 'edit';
  }

  async function deleteConnection() {
    if (!selectedId) return;

    const confirmMessage = get(t)('connections.delete.confirm', {
      values: { name: selectedId },
    });

    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      await api.connections.delete(selectedId);
      showDeleteSuccessToast('connections', selectedId);
      await loadConnections();

      selectedConnection = null;
      selectedId = null;
      viewMode = 'detail';
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'connections',
      );
    }
  }

  async function saveConnection(event: CustomEvent) {
    const data = event.detail;

    try {
      if (formMode === 'create') {
        await api.connections.create(data);
        showFormSuccessToast({
          entityType: 'connections',
          mode: 'create',
          entityName: data.name,
        });
      } else if (selectedId) {
        await api.connections.update(selectedId, data.config);
        showFormSuccessToast({
          entityType: 'connections',
          mode: 'edit',
          entityName: selectedId,
        });
      }

      await loadConnections();

      if (formMode === 'create' && data.name) {
        await selectConnection(data.name);
      } else {
        viewMode = 'detail';
        if (selectedId) {
          const details = await api.connections.get(selectedId);
          selectedConnection = {
            name: selectedId,
            config: details,
          };
        }
      }
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'connections',
      );
    }
  }

  onMount(() => {
    loadConnections();
  });
</script>

<Container>
  <List slot="list" {loading}>
    <ListHeader title={$t('connections.title')} onCreate={createNew}>
      {#snippet buttonText()}
        {$t('connections.create')}
      {/snippet}
    </ListHeader>

    {#each connections as connection}
      <ListItem
        id={connection.name}
        title={connection.name}
        description={connection.details}
        selected={selectedId === connection.name}
        onclick={() => selectConnection(connection.name)}
      />
    {/each}

    {#if connections.length === 0 && !loading}
      <div class="empty-list">
        <p>{$t('connections.empty.message')}</p>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if viewMode === 'detail' && selectedConnection}
      <ConnectionDetail
        entity={selectedConnection}
        on:edit={editConnection}
        on:delete={deleteConnection}
      />
    {:else if viewMode === 'form'}
      <ConnectionForm
        mode={formMode}
        initialData={formMode === 'edit' ? selectedConnection : null}
        on:submit={saveConnection}
        on:close={() => {
          viewMode = 'detail';
          if (!selectedId) {
            selectedConnection = null;
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

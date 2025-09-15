<script lang="ts">
  import { onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { t } from '../i18n';
  import { api } from '../api';
  import type { QuerySummary, QueryConfig } from '../api';
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

  import QueryDetail from '../components/show/Query.svelte';
  import QueryForm from '../components/form/Query.svelte';

  let queries: QuerySummary[] = $state([]);
  let selectedQuery: QueryConfig | null = $state(null);
  let selectedId: string | null = $state(null);
  let viewMode: 'detail' | 'form' = $state('detail');
  let formMode: 'create' | 'edit' = $state('create');
  let loading = $state(false);

  async function loadQueries() {
    loading = true;
    queries = await api.queries.list();
    loading = false;
  }

  async function selectQuery(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    viewMode = 'detail';
    selectedQuery = await api.queries.get(id);
  }

  function createNew() {
    selectedQuery = null;
    selectedId = null;
    viewMode = 'form';
    formMode = 'create';
  }

  function editQuery() {
    viewMode = 'form';
    formMode = 'edit';
  }

  async function deleteQuery() {
    if (!selectedId) return;

    const confirmMessage = get(t)('query.delete_confirm', {
      values: { name: selectedId },
    });

    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      await api.queries.delete(selectedId);
      const message = get(t)('query.delete_success', {
        values: { name: selectedId },
      });
      window.showToast?.success(message);
      await loadQueries();

      selectedQuery = null;
      selectedId = null;
      viewMode = 'detail';
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'query',
      );
    }
  }

  async function saveQuery(event: CustomEvent) {
    const data = event.detail;

    try {
      if (formMode === 'create') {
        await api.queries.save(data);
        const message = get(t)('query.create_success', {
          values: { name: data.name },
        });
        window.showToast?.success(message);
      } else if (selectedId) {
        await api.queries.update(selectedId, {
          name: selectedId,
          description: data.description,
          sql: data.sql,
        });
        const message = get(t)('query.edit_success', {
          values: { name: selectedId },
        });
        window.showToast?.success(message);
      }

      await loadQueries();

      if (formMode === 'create' && data.name) {
        await selectQuery(data.name);
      } else {
        viewMode = 'detail';
        if (selectedId) {
          selectedQuery = await api.queries.get(selectedId);
        }
      }
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'query',
      );
    }
  }

  onMount(() => {
    loadQueries();
  });
</script>

<Container>
  <List slot="list" {loading}>
    <ListHeader title={$t('query.saved_queries')} onCreate={createNew}>
      {#snippet buttonText()}
        {$t('query.create')}
      {/snippet}
    </ListHeader>

    {#each queries as query}
      <ListItem
        id={query.name}
        title={query.name}
        description={query.description}
        selected={selectedId === query.name}
        onclick={() => selectQuery(query.name)}
      />
    {/each}

    {#if queries.length === 0 && !loading}
      <div class="empty-list">
        <p>{$t('query.empty')}</p>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if viewMode === 'detail' && selectedQuery}
      <QueryDetail
        entity={selectedQuery}
        on:edit={editQuery}
        on:delete={deleteQuery}
      />
    {:else if viewMode === 'form'}
      <QueryForm
        mode={formMode}
        initialData={formMode === 'edit' ? selectedQuery : null}
        on:submit={saveQuery}
        on:close={() => {
          viewMode = 'detail';
          if (!selectedId) {
            selectedQuery = null;
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

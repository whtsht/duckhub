<script lang="ts">
  import { onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { t } from '../i18n';
  import { api } from '../api';
  import type { DashboardSummary, DashboardConfig } from '../api';
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

  import DashboardDetail from '../components/show/Dashboard.svelte';
  import DashboardForm from '../components/form/Dashboard.svelte';

  let dashboards: DashboardSummary[] = $state([]);
  let selectedDashboard: DashboardConfig | null = $state(null);
  let selectedId: string | null = $state(null);
  let viewMode: 'detail' | 'form' = $state('detail');
  let formMode: 'create' | 'edit' = $state('create');
  let loading = $state(false);

  async function loadDashboards() {
    loading = true;
    dashboards = await api.dashboards.list();
    loading = false;
  }

  async function selectDashboard(id: string) {
    if (selectedId === id) return;

    selectedId = id;
    viewMode = 'detail';
    selectedDashboard = await api.dashboards.get(id);
  }

  function createNew() {
    selectedDashboard = null;
    selectedId = null;
    viewMode = 'form';
    formMode = 'create';
  }

  function editDashboard() {
    viewMode = 'form';
    formMode = 'edit';
  }

  async function deleteDashboard() {
    if (!selectedId) return;

    const confirmMessage = get(t)('dashboards.delete.confirm');

    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      await api.dashboards.delete(selectedId);
      showDeleteSuccessToast('dashboards', selectedId);
      await loadDashboards();

      selectedDashboard = null;
      selectedId = null;
      viewMode = 'detail';
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'dashboards',
      );
    }
  }

  async function saveDashboard(event: CustomEvent) {
    const data = event.detail;
    if (!data) return;

    try {
      if (formMode === 'create') {
        await api.dashboards.create(data);
        showFormSuccessToast({
          entityType: 'dashboards',
          mode: 'create',
          entityName: data.name,
        });
      } else if (selectedId) {
        await api.dashboards.update(selectedId, data);
        showFormSuccessToast({
          entityType: 'dashboards',
          mode: 'edit',
          entityName: selectedId,
        });
      }

      await loadDashboards();

      if (formMode === 'create' && data.name) {
        await selectDashboard(data.name);
      } else {
        viewMode = 'detail';
        if (selectedId) {
          selectedDashboard = await api.dashboards.get(selectedId);
        }
      }
    } catch (error) {
      showFormErrorToast(
        error instanceof Error ? error.message : 'Unknown error',
        'dashboards',
      );
    }
  }

  onMount(() => {
    loadDashboards();
  });
</script>

<Container>
  <List slot="list" {loading}>
    <ListHeader title={$t('dashboards.title')} onCreate={createNew}>
      {#snippet buttonText()}
        {$t('dashboards.new.button')}
      {/snippet}
    </ListHeader>

    {#each dashboards as dashboard}
      <ListItem
        id={dashboard.name}
        title={dashboard.name}
        description={dashboard.description}
        selected={selectedId === dashboard.name}
        onclick={() => selectDashboard(dashboard.name)}
      />
    {/each}

    {#if dashboards.length === 0 && !loading}
      <div class="empty-list">
        <p>{$t('dashboards.empty')}</p>
      </div>
    {/if}
  </List>

  <MainPanel slot="main">
    {#if viewMode === 'detail' && selectedDashboard}
      <DashboardDetail
        entity={selectedDashboard && selectedId
          ? { name: selectedId, config: selectedDashboard }
          : null}
        on:edit={editDashboard}
        on:delete={deleteDashboard}
      />
    {:else if viewMode === 'form'}
      <DashboardForm
        mode={formMode}
        initialData={formMode === 'edit' ? selectedDashboard : null}
        on:submit={saveDashboard}
        on:close={() => {
          viewMode = 'detail';
          if (!selectedId) {
            selectedDashboard = null;
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

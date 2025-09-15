<script lang="ts">
  import './lib/i18n';
  import { isLoading, t } from './lib/i18n';
  import Navigation from './lib/Navigation.svelte';
  import ToastContainer from './lib/components/ToastContainer.svelte';

  import AdapterPanel from './lib/panels/AdapterPanel.svelte';
  import ConnectionPanel from './lib/panels/ConnectionPanel.svelte';
  import ModelPanel from './lib/panels/ModelPanel.svelte';
  import QueryPanel from './lib/panels/QueryPanel.svelte';
  import DashboardPanel from './lib/panels/DashboardPanel.svelte';
  import GraphPanel from './lib/panels/GraphPanel.svelte';
  import PipelineHistoryPanel from './lib/panels/PipelineHistoryPanel.svelte';
  import SettingsPanel from './lib/SettingsPanel.svelte';

  let activeSection = $state('connections');
  let toastContainer: ToastContainer;
</script>

{#if !$isLoading}
  <div class="app">
    <Navigation bind:activeSection />

    <main class="main-content">
      {#if activeSection === 'connections'}
        <ConnectionPanel />
      {:else if activeSection === 'adapters'}
        <AdapterPanel />
      {:else if activeSection === 'models'}
        <ModelPanel />
      {:else if activeSection === 'dashboards'}
        <DashboardPanel />
      {:else if activeSection === 'query'}
        <QueryPanel />
      {:else if activeSection === 'graph'}
        <GraphPanel />
      {:else if activeSection === 'pipeline'}
        <PipelineHistoryPanel />
      {:else if activeSection === 'settings'}
        <SettingsPanel />
      {/if}
    </main>
  </div>
{:else}
  <div class="loading">{$t('common.loading')}</div>
{/if}

<ToastContainer bind:this={toastContainer} />

<style>
  .app {
    display: flex;
    height: 100vh;
    background-color: var(--color-background);
  }

  .main-content {
    flex: 1;
    overflow: hidden;
  }

  .loading {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh;
    font-size: 1.2rem;
    color: var(--color-text-secondary);
  }
</style>

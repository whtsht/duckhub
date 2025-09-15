<script lang="ts">
  import {
    Database,
    FileText,
    Settings,
    Link,
    Search,
    GitBranch,
    BarChart3,
    History,
  } from 'lucide-svelte';
  import { t } from './i18n';

  let {
    activeSection = $bindable('connections'),
  }: {
    activeSection?: string;
  } = $props();
</script>

<nav class="navigation">
  <ul class="nav-items">
    <li class="nav-item" class:active={activeSection === 'connections'}>
      <button
        onclick={() => (activeSection = 'connections')}
        title={$t('navigation.connections')}
      >
        <Link class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'adapters'}>
      <button
        onclick={() => (activeSection = 'adapters')}
        title={$t('navigation.adapters')}
      >
        <Database class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'models'}>
      <button
        onclick={() => (activeSection = 'models')}
        title={$t('navigation.models')}
      >
        <FileText class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'graph'}>
      <button
        onclick={() => (activeSection = 'graph')}
        title={$t('navigation.graph')}
      >
        <GitBranch class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'pipeline'}>
      <button
        onclick={() => (activeSection = 'pipeline')}
        title={$t('navigation.pipeline')}
      >
        <History class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'query'}>
      <button
        onclick={() => (activeSection = 'query')}
        title={$t('navigation.query')}
      >
        <Search class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'dashboards'}>
      <button
        onclick={() => (activeSection = 'dashboards')}
        title={$t('navigation.dashboards')}
      >
        <BarChart3 class="icon" />
      </button>
    </li>
    <li class="nav-item" class:active={activeSection === 'settings'}>
      <button
        onclick={() => (activeSection = 'settings')}
        title={$t('navigation.settings')}
      >
        <Settings class="icon" />
      </button>
    </li>
  </ul>
</nav>

<style>
  .navigation {
    width: 60px;
    height: 100vh;
    background-color: var(--color-background);
    border-right: 1px solid var(--color-border);
    display: flex;
    flex-direction: column;
  }

  .nav-items {
    list-style: none;
    padding: 8px 0;
    margin: 0;
  }

  .nav-item {
    margin: 2px 0;
  }

  .nav-item button {
    width: 100%;
    height: 60px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: none;
    border: none;
    cursor: pointer;
    transition: all 0.2s;
    color: var(--color-text-secondary);
    position: relative;
  }

  .nav-item button:hover {
    background-color: var(--color-background-selected);
    color: var(--color-primary);
  }

  .nav-item.active button {
    background-color: var(--color-background-selected);
    color: var(--color-primary);
  }

  .nav-item.active button::before {
    content: '';
    position: absolute;
    left: 0;
    top: 50%;
    transform: translateY(-50%);
    width: 3px;
    height: 24px;
    background-color: var(--color-primary);
    border-radius: 0 2px 2px 0;
  }

  :global(.icon) {
    width: 24px;
    height: 24px;
    flex-shrink: 0;
  }
</style>

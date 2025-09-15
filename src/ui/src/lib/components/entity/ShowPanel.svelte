<script lang="ts">
  import { Edit, Trash2 } from 'lucide-svelte';
  import { t } from '../../i18n';
  import { Button } from '../common';
  import type { Snippet } from 'svelte';

  let {
    title,
    handleEdit,
    handleDelete,
    loading = false,
    children,
  }: {
    title: string;
    handleEdit?: () => void;
    handleDelete?: () => void;
    loading?: boolean;
    children?: Snippet;
  } = $props();
</script>

<div class="show-panel">
  <header class="panel-header">
    <div class="title-section">
      <h2 class="panel-title">{title}</h2>
    </div>

    <div class="actions">
      {#if handleEdit}
        <Button icon={Edit} onclick={handleEdit} disabled={loading}>
          {$t('common.edit')}
        </Button>
      {/if}

      {#if handleDelete}
        <Button icon={Trash2} onclick={handleDelete} disabled={loading}>
          {$t('common.delete')}
        </Button>
      {/if}
    </div>
  </header>

  <div class="panel-content">
    {@render children?.()}
  </div>

  {#if loading}
    <div class="loading-overlay">
      <div class="loading-spinner"></div>
      <p>{$t('common.loading')}</p>
    </div>
  {/if}
</div>

<style>
  .show-panel {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: white;
    position: relative;
  }

  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px 24px;
    border-bottom: 1px solid var(--color-border);
    background: var(--color-background-hover);
  }

  .title-section {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .panel-title {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--color-text-dark);
  }

  .actions {
    display: flex;
    gap: 8px;
    align-items: center;
  }

  .panel-content {
    flex: 1;
    padding: 24px;
    overflow-y: auto;
  }

  .loading-overlay {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(255, 255, 255, 0.9);
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    z-index: 10;
  }

  .loading-spinner {
    width: 32px;
    height: 32px;
    border: 3px solid #f3f3f3;
    border-top: 3px solid var(--color-primary);
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin-bottom: 12px;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }
</style>

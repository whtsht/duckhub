<script lang="ts">
  import { t } from '../../i18n';
  import type { Snippet } from 'svelte';

  let {
    loading = false,
    children,
  }: {
    loading?: boolean;
    children?: Snippet;
  } = $props();
</script>

<div class="entity-list">
  {#if loading}
    <div class="loading">{$t('common.loading')}</div>
  {:else}
    {@render children?.()}
  {/if}
</div>

<style>
  .entity-list {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: white;
  }

  .loading {
    padding: 24px;
    text-align: center;
    color: var(--color-text-secondary);
  }
</style>

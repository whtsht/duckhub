<script lang="ts">
  import { Button } from '../common';
  import { Plus } from 'lucide-svelte';
  import type { Snippet } from 'svelte';
  import { t } from '../../i18n';

  let {
    title,
    onCreate,
    buttonText,
  }: {
    title: string;
    onCreate: () => void;
    buttonText?: Snippet;
  } = $props();
</script>

<div class="list-header">
  <h3>{title}</h3>
  <Button icon={Plus} onclick={onCreate}>
    {#if buttonText}
      {@render buttonText()}
    {:else}
      {$t('common.create')}
    {/if}
  </Button>
</div>

<style>
  .list-header {
    padding: 16px;
    border-bottom: 1px solid var(--color-border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: white;
  }

  h3 {
    margin: 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
    color: var(--color-text-primary);
  }
</style>

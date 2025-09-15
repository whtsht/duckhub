<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { t } from './i18n';
  import { Button } from './components/common';
  import { Plus } from 'lucide-svelte';

  const dispatch = createEventDispatcher();

  let {
    messageKey,
    actionKey,
  }: {
    messageKey: string;
    actionKey: string;
  } = $props();

  function handleCreate() {
    dispatch('create');
  }
</script>

<div class="empty-state">
  <p>{$t(messageKey)}</p>
  <Button icon={Plus} onclick={handleCreate}>
    {$t(actionKey)}
  </Button>
</div>

<style>
  .empty-state {
    padding: 40px 16px;
    text-align: center;
    color: var(--color-text-secondary);
  }

  .empty-state p {
    margin-bottom: 16px;
  }
</style>

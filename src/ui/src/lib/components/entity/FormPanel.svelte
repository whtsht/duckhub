<script lang="ts">
  import { X } from 'lucide-svelte';
  import { Button } from '../common';
  import { t } from '../../i18n';
  import type { Snippet } from 'svelte';

  let {
    handleSubmit,
    handleCancel,
    children,
    actions,
  }: {
    handleSubmit: () => void;
    handleCancel: () => void;
    children?: Snippet;
    actions?: Snippet;
  } = $props();
</script>

<div class="form-panel">
  <header class="form-header">
    <div class="actions">
      <Button icon={X} onclick={handleCancel}>
        {$t('common.cancel')}
      </Button>
    </div>
  </header>

  <div class="form-content">
    {@render children?.()}
  </div>

  <footer class="form-footer">
    <div class="footer-actions">
      {@render actions?.()}
    </div>
  </footer>
</div>

<style>
  .form-panel {
    height: 100%;
    display: flex;
    flex-direction: column;
    background: white;
  }

  .form-header {
    padding: 16px 24px;
    border-bottom: 1px solid var(--color-border);
    display: flex;
    justify-content: flex-end;
  }

  .actions {
    display: flex;
    gap: 8px;
  }

  .form-content {
    flex: 1;
    padding: 24px;
    overflow-y: auto;
  }

  .form-footer {
    padding: 16px 24px;
    border-top: 1px solid var(--color-border);
    background: var(--color-background-hover);
  }

  .footer-actions {
    display: flex;
    justify-content: flex-end;
    gap: 12px;
  }
</style>

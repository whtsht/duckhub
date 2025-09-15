<script lang="ts">
  import { AlertCircle } from 'lucide-svelte';

  export let message: string;
  export let monospace: boolean = false;
</script>

<div class="error-container">
  <div class="error-content">
    <AlertCircle class="error-icon" size={16} />
    <div class="error-message" class:monospace>{message}</div>
  </div>
</div>

<style>
  .error-container {
    background-color: #fee;
    border: 1px solid #fcc;
    border-radius: 4px;
    padding: 12px;
  }

  .error-content {
    display: flex;
    align-items: flex-start;
    gap: 8px;
  }

  :global(.error-icon) {
    color: #c33;
    flex-shrink: 0;
    margin-top: 1px;
  }

  .error-message {
    color: var(--color-text-secondary);
    white-space: pre-wrap;
    font-size: 13px;
    margin: 0;
    overflow-x: auto;
  }

  .error-message.monospace {
    font-family: monospace;
  }
</style>

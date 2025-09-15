<script lang="ts">
</script>

<div class="entity-container">
  <div class="entity-list-panel">
    <slot name="list" />
  </div>
  <div class="entity-main-panel">
    <slot name="main" />
  </div>
</div>

<style>
  .entity-container {
    display: flex;
    height: 100vh;
  }

  .entity-list-panel {
    width: 320px;
    flex-shrink: 0;
    border-right: 1px solid var(--color-border);
    overflow-y: auto;
    background: var(--color-background-hover);
  }

  .entity-main-panel {
    flex: 1;
    overflow-y: auto;
    background: white;
  }
</style>

<script lang="ts">
  let {
    id,
    title,
    description = '',
    selected = false,
    onclick,
  }: {
    id: string;
    title: string;
    description?: string | null;
    selected?: boolean;
    onclick: () => void;
  } = $props();
</script>

<button
  class="list-item"
  class:selected
  {onclick}
  role="option"
  aria-selected={selected}
  data-id={id}
>
  <div class="item-content">
    <p class="item-title">{title}</p>
    {#if description}
      <p class="item-description">{description}</p>
    {/if}
  </div>
</button>

<style>
  .list-item {
    width: 100%;
    padding: 12px 16px;
    background: white;
    border: none;
    border-bottom: 1px solid #f0f0f0;
    cursor: pointer;
    text-align: left;
    transition: background-color 0.2s;
  }

  .list-item:hover {
    background: var(--color-background-hover);
  }

  .list-item.selected {
    background: var(--color-background-selected);
    border-left: 3px solid var(--color-primary);
    padding-left: 13px;
  }

  .list-item:focus {
    outline: 2px solid var(--color-primary);
    outline-offset: -2px;
  }

  .item-content {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .item-title {
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-primary);
    margin: 0;
  }

  .item-description {
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
    color: var(--color-text-muted);
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>

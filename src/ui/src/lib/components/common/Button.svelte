<script lang="ts">
  import type { ComponentType } from 'svelte';

  interface Props {
    icon?: ComponentType;
    disabled?: boolean;
    onclick?: () => void;
    type?: 'button' | 'submit' | 'reset';
    children?: any;
  }

  let {
    icon: Icon,
    disabled = false,
    onclick,
    type = 'button',
    children,
  }: Props = $props();
</script>

<button class="btn" {disabled} {onclick} {type}>
  {#if Icon}
    <Icon class="btn-icon" size={18} />
  {/if}
  {#if children}
    <span class="btn-text">{@render children()}</span>
  {/if}
</button>

<style>
  .btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.375rem;
    padding: 0.5rem 1rem;
    font-family: inherit;
    font-size: var(--font-button-size);
    font-weight: var(--font-button-weight);
    line-height: var(--font-button-line-height);
    border-radius: 6px;
    cursor: pointer;
    transition: all 0.2s ease;
    background-color: transparent;
    color: var(--color-primary);
    border: 1px solid var(--color-primary);
    outline: none;
  }

  .btn:focus-visible {
    box-shadow: 0 0 0 2px rgba(39, 174, 96, 0.2);
  }

  .btn:hover:not(:disabled) {
    background-color: rgba(39, 174, 96, 0.1);
    border-color: var(--color-primary-hover);
    color: var(--color-primary-hover);
  }

  .btn:active:not(:disabled) {
    background-color: rgba(39, 174, 96, 0.2);
    border-color: var(--color-primary-active);
    color: var(--color-primary-active);
  }

  .btn:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }

  .btn :global(.btn-icon) {
    flex-shrink: 0;
  }

  .btn-text {
    white-space: nowrap;
  }
</style>

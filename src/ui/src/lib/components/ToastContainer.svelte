<script module lang="ts">
  export type ToastType = 'success' | 'error' | 'warning' | 'info';

  export interface Toast {
    id: number;
    message: string;
    type: ToastType;
    duration: number;
  }
</script>

<script lang="ts">
  import { fly } from 'svelte/transition';
  import { CheckCircle, XCircle, AlertCircle, Info, X } from 'lucide-svelte';
  import { t } from '../i18n';

  let toasts: Toast[] = $state([]);
  let idCounter = 0;
  const maxToasts = 5;
  let timers = new Map<number, number>();

  if (typeof window !== 'undefined') {
    window.showToast = {
      success: (message: string, duration?: number) =>
        toastStore.success(message, duration),
      error: (message: string, duration?: number) =>
        toastStore.error(message, duration),
      warning: (message: string, duration?: number) =>
        toastStore.warning(message, duration),
      info: (message: string, duration?: number) =>
        toastStore.info(message, duration),
    };
  }

  export const toastStore = {
    get items() {
      return toasts;
    },

    add(message: string, type: ToastType = 'info', duration: number) {
      const id = idCounter++;
      const toast: Toast = { id, message, type, duration };

      toasts.push(toast);

      if (toasts.length > maxToasts) {
        toasts.shift();
      }

      if (duration > 0) {
        const timerId = setTimeout(() => {
          this.remove(id);
        }, duration);
        timers.set(id, timerId);
      }

      return id;
    },

    remove(id: number) {
      const timerId = timers.get(id);
      if (timerId) {
        clearTimeout(timerId);
        timers.delete(id);
      }

      const index = toasts.findIndex((t) => t.id === id);
      if (index > -1) {
        toasts.splice(index, 1);
      }
    },

    pauseTimer(id: number) {
      const timerId = timers.get(id);
      if (timerId) {
        clearTimeout(timerId);
        timers.delete(id);
      }
    },

    resumeTimer(id: number, duration: number) {
      const timerId = setTimeout(() => {
        this.remove(id);
      }, duration);
      timers.set(id, timerId);
    },

    clear() {
      toasts.length = 0;
    },

    success(message: string, duration?: number) {
      return this.add(message, 'success', duration ?? 2000);
    },

    error(message: string, duration?: number) {
      return this.add(message, 'error', duration ?? 5000);
    },

    warning(message: string, duration?: number) {
      return this.add(message, 'warning', duration ?? 5000);
    },

    info(message: string, duration?: number) {
      return this.add(message, 'info', duration ?? 5000);
    },
  };

  function getToastIcon(type: Toast['type']) {
    switch (type) {
      case 'success':
        return CheckCircle;
      case 'error':
        return XCircle;
      case 'warning':
        return AlertCircle;
      case 'info':
        return Info;
    }
  }

  function getToastClasses(type: Toast['type']) {
    switch (type) {
      case 'success':
        return 'toast-item toast-success';
      case 'error':
        return 'toast-item toast-error';
      case 'warning':
        return 'toast-item toast-warning';
      case 'info':
        return 'toast-item toast-info';
    }
  }

  function getIconClasses(type: Toast['type']) {
    switch (type) {
      case 'success':
        return 'toast-icon-success';
      case 'error':
        return 'toast-icon-error';
      case 'warning':
        return 'toast-icon-warning';
      case 'info':
        return 'toast-icon-info';
    }
  }

  function handleMouseEnter(toast: Toast) {
    toastStore.pauseTimer(toast.id);
  }

  function handleMouseLeave(toast: Toast) {
    toastStore.resumeTimer(toast.id, toast.duration);
  }
</script>

<div class="toast-container">
  {#each toasts as toast (toast.id)}
    <div
      class={getToastClasses(toast.type)}
      role="alert"
      aria-live="polite"
      in:fly={{ y: 20, duration: 300 }}
      out:fly={{ y: 20, duration: 200 }}
      onmouseenter={() => handleMouseEnter(toast)}
      onmouseleave={() => handleMouseLeave(toast)}
    >
      {#snippet toastIcon(type: ToastType)}
        {@const Icon = getToastIcon(type)}
        <Icon class="toast-icon {getIconClasses(type)}" size={20} />
      {/snippet}
      {@render toastIcon(toast.type)}

      <div class="toast-content">
        <p class="toast-message">{toast.message}</p>
      </div>

      <button
        class="toast-close"
        onclick={() => toastStore.remove(toast.id)}
        aria-label={$t('common.close_toast')}
      >
        <X size={16} />
      </button>
    </div>
  {/each}
</div>

<style>
  .toast-container {
    position: fixed;
    bottom: 2rem;
    left: 50%;
    transform: translateX(-50%);
    z-index: 9999;
    display: flex;
    flex-direction: column-reverse;
    gap: 0.75rem;
    max-width: 36rem;
    pointer-events: none;
  }

  .toast-item {
    pointer-events: auto;
    min-width: 16rem;
    cursor: pointer;
    transition: transform 0.2s ease-in-out;
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 16px;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    border: 1px solid;
  }

  .toast-item:hover {
    transform: translateY(-2px);
  }

  .toast-success {
    background-color: var(--color-success-bg);
    border-color: var(--color-success-border);
    color: var(--color-success-text);
  }

  .toast-error {
    background-color: var(--color-error-bg);
    border-color: var(--color-error-border);
    color: var(--color-error-text);
  }

  .toast-warning {
    background-color: var(--color-warning-bg);
    border-color: var(--color-warning-border);
    color: var(--color-warning-text);
  }

  .toast-info {
    background-color: var(--color-info-bg);
    border-color: var(--color-info-border);
    color: var(--color-info-text);
  }

  .toast-icon {
    flex-shrink: 0;
    margin-top: 1px;
  }

  .toast-icon-success {
    color: var(--color-success-light);
  }

  .toast-icon-error {
    color: var(--color-error-light);
  }

  .toast-icon-warning {
    color: var(--color-warning-light);
  }

  .toast-icon-info {
    color: var(--color-info-light);
  }

  .toast-content {
    flex: 1;
    min-width: 0;
  }

  .toast-message {
    margin: 0;
    font-size: 0.875rem;
    line-height: 1.4;
    word-wrap: break-word;
  }

  .toast-close {
    flex-shrink: 0;
    padding: 0.125rem;
    border: none;
    background: none;
    cursor: pointer;
    border-radius: 0.25rem;
    opacity: 0.5;
    transition: opacity 0.2s ease;
  }

  .toast-close:hover {
    opacity: 1;
  }

  .toast-close:focus {
    outline: 2px solid currentColor;
    outline-offset: 1px;
  }
</style>

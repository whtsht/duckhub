<script lang="ts">
  import { RefreshCw } from 'lucide-svelte';
  import { t } from '../../i18n';
  import Button from './Button.svelte';

  interface Props {
    onRefresh: () => void | Promise<void>;
    loading?: boolean;
    disabled?: boolean;
  }

  let { onRefresh, loading = false, disabled = false }: Props = $props();
</script>

<Button icon={RefreshCw} onclick={onRefresh} disabled={loading || disabled}>
  {$t('graph.refresh')}
</Button>

<script lang="ts">
  import { Button } from '../common';
  import { CircleCheckBig } from 'lucide-svelte';
  import { get } from 'svelte/store';
  import { t } from '../../i18n';
  import {
    api,
    type ConnectionConfig,
    type TestConnectionConfig,
  } from '../../api';

  interface Props {
    connectionConfig: any;
    disabled?: boolean;
    onTestComplete?: (success: boolean) => void;
  }

  let { connectionConfig, disabled = false, onTestComplete }: Props = $props();

  let isTestingConnection = $state(false);

  function buildTestConnectionConfig(
    config: ConnectionConfig,
  ): TestConnectionConfig | null {
    switch (config.type) {
      case 'sqlite':
        return {
          type: 'sqlite' as const,
          path: config.config.path,
        };
      case 'localfile':
        return {
          type: 'localfile' as const,
          base_path: config.config.base_path,
        };
      case 'mysql':
        return {
          type: 'mysql' as const,
          host: config.config.host,
          port: config.config.port,
          database: config.config.database,
          username: config.config.username,
          password:
            config.config.password?.type === 'plain'
              ? config.config.password.value
              : '',
        };
      case 'postgresql':
        return {
          type: 'postgresql' as const,
          host: config.config.host,
          port: config.config.port,
          database: config.config.database,
          username: config.config.username,
          password:
            config.config.password?.type === 'plain'
              ? config.config.password.value
              : '',
        };
      case 's3':
        return {
          type: 's3' as const,
          bucket: config.config.bucket,
          region: config.config.region,
          endpoint_url: config.config.endpoint_url || undefined,
          auth_method: config.config.auth_method,
          access_key_id: config.config.access_key_id || undefined,
          secret_access_key:
            config.config.secret_access_key?.type === 'plain'
              ? config.config.secret_access_key.value
              : undefined,
          path_style_access: config.config.path_style_access,
        };
      default:
        return null;
    }
  }

  async function handleTestConnection() {
    const testConfig = buildTestConnectionConfig(connectionConfig);
    if (!testConfig) {
      window.showToast?.error(
        get(t)('connections.form.test_error', {
          values: { error: 'Invalid connection configuration' },
        }),
      );
      return;
    }

    isTestingConnection = true;
    try {
      await api.connections.test(testConfig);
      const message = get(t)('connections.form.test_success');
      window.showToast?.success(message);
      onTestComplete?.(true);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const message = get(t)('connections.form.test_error', {
        values: { error: errorMessage },
      });
      window.showToast?.error(message);
      onTestComplete?.(false);
    } finally {
      isTestingConnection = false;
    }
  }
</script>

<Button
  icon={CircleCheckBig}
  onclick={handleTestConnection}
  disabled={disabled || isTestingConnection || !connectionConfig}
>
  {$t(
    isTestingConnection
      ? 'connections.form.testing'
      : 'connections.form.test_connection',
  )}
</Button>

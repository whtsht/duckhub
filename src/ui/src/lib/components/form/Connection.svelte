<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ConnectionConfig } from '../../api';
  import FormPanel from '../entity/FormPanel.svelte';
  import { t } from '../../i18n';
  import { Button, ConnectionTestButton } from '../common';
  import { Database, Folder, Save } from 'lucide-svelte';

  const dispatch = createEventDispatcher();

  let {
    mode = 'create',
    initialData = null,
  }: {
    mode?: 'create' | 'edit';
    initialData?: { name: string; config: ConnectionConfig } | null;
  } = $props();

  let name = $state('');
  let connectionType = $state('sqlite');

  let sqlitePath = $state('');
  let localfilePath = $state('');
  let mysqlHost = $state('localhost');
  let mysqlPort = $state(3306);
  let mysqlDatabase = $state('');
  let mysqlUsername = $state('');
  let mysqlPassword = $state('');
  let postgresHost = $state('localhost');
  let postgresPort = $state(5432);
  let postgresDatabase = $state('');
  let postgresUsername = $state('');
  let postgresPassword = $state('');
  let s3Bucket = $state('');
  let s3Region = $state('us-east-1');
  let s3EndpointUrl = $state('');
  let s3AuthMethod = $state<'credential_chain' | 'explicit'>(
    'credential_chain',
  );
  let s3AccessKeyId = $state('');
  let s3SecretAccessKey = $state('');
  let s3PathStyleAccess = $state(false);

  let hasTestedSuccessfully = $state(false);

  $effect(() => {
    if (initialData && mode === 'edit') {
      name = initialData.name;
      connectionType = initialData.config.type;

      if (connectionType === 'sqlite' && initialData.config.type === 'sqlite') {
        sqlitePath = initialData.config.config.path || '';
      } else if (
        connectionType === 'localfile' &&
        initialData.config.type === 'localfile'
      ) {
        localfilePath = initialData.config.config.base_path || '';
      } else if (
        connectionType === 'mysql' &&
        initialData.config.type === 'mysql'
      ) {
        mysqlHost = initialData.config.config.host || 'localhost';
        mysqlPort = initialData.config.config.port || 3306;
        mysqlDatabase = initialData.config.config.database || '';
        mysqlUsername = initialData.config.config.username || '';
        mysqlPassword =
          (initialData.config.config.password?.type === 'plain'
            ? initialData.config.config.password.value
            : '') || '';
      } else if (
        connectionType === 'postgresql' &&
        initialData.config.type === 'postgresql'
      ) {
        postgresHost = initialData.config.config.host || 'localhost';
        postgresPort = initialData.config.config.port || 5432;
        postgresDatabase = initialData.config.config.database || '';
        postgresUsername = initialData.config.config.username || '';
        postgresPassword =
          (initialData.config.config.password?.type === 'plain'
            ? initialData.config.config.password.value
            : '') || '';
      } else if (connectionType === 's3' && initialData.config.type === 's3') {
        s3Bucket = initialData.config.config.bucket || '';
        s3Region = initialData.config.config.region || 'us-east-1';
        s3EndpointUrl = initialData.config.config.endpoint_url || '';
        s3AuthMethod =
          initialData.config.config.auth_method || 'credential_chain';
        s3AccessKeyId = initialData.config.config.access_key_id || '';
        s3SecretAccessKey =
          (initialData.config.config.secret_access_key?.type === 'plain'
            ? initialData.config.config.secret_access_key.value
            : '') || '';
        s3PathStyleAccess =
          initialData.config.config.path_style_access || false;
      }
    }
  });

  // Reset test success state when form values change
  $effect(() => {
    // Track all form fields
    connectionType;
    sqlitePath;
    localfilePath;
    mysqlHost;
    mysqlPort;
    mysqlDatabase;
    mysqlUsername;
    mysqlPassword;
    postgresHost;
    postgresPort;
    postgresDatabase;
    postgresUsername;
    postgresPassword;
    s3Bucket;
    s3Region;
    s3EndpointUrl;
    s3AuthMethod;
    s3AccessKeyId;
    s3SecretAccessKey;
    s3PathStyleAccess;

    // Reset test success state when any field changes
    hasTestedSuccessfully = false;
  });

  function handleSubmit() {
    const config = buildConnectionConfig();
    dispatch('submit', { name, config });
  }

  function handleCancel() {
    name = '';
    connectionType = 'sqlite';
    sqlitePath = '';
    localfilePath = '';
    mysqlHost = 'localhost';
    mysqlPort = 3306;
    mysqlDatabase = '';
    mysqlUsername = '';
    mysqlPassword = '';
    postgresHost = 'localhost';
    postgresPort = 5432;
    postgresDatabase = '';
    postgresUsername = '';
    postgresPassword = '';
    s3Bucket = '';
    s3Region = 'us-east-1';
    s3EndpointUrl = '';
    s3AuthMethod = 'credential_chain';
    s3AccessKeyId = '';
    s3SecretAccessKey = '';
    s3PathStyleAccess = false;
    dispatch('close');
  }

  function isFormValid() {
    if (!name.trim()) return false;

    switch (connectionType) {
      case 'sqlite':
        return !!sqlitePath.trim();
      case 'localfile':
        return !!localfilePath.trim();
      case 'mysql':
        return !!mysqlDatabase.trim() && !!mysqlUsername.trim();
      case 'postgresql':
        return !!postgresDatabase.trim() && !!postgresUsername.trim();
      case 's3':
        return (
          !!s3Bucket.trim() &&
          (s3AuthMethod === 'credential_chain' ||
            (!!s3AccessKeyId.trim() && !!s3SecretAccessKey.trim()))
        );
      default:
        return false;
    }
  }

  function buildConnectionConfig() {
    let innerConfig: any = {};

    if (connectionType === 'sqlite') {
      innerConfig.path = sqlitePath;
    } else if (connectionType === 'localfile') {
      innerConfig.base_path = localfilePath;
    } else if (connectionType === 'mysql') {
      innerConfig.host = mysqlHost;
      innerConfig.port = mysqlPort;
      innerConfig.database = mysqlDatabase;
      innerConfig.username = mysqlUsername;
      innerConfig.password = { type: 'plain', value: mysqlPassword };
    } else if (connectionType === 'postgresql') {
      innerConfig.host = postgresHost;
      innerConfig.port = postgresPort;
      innerConfig.database = postgresDatabase;
      innerConfig.username = postgresUsername;
      innerConfig.password = { type: 'plain', value: postgresPassword };
    } else if (connectionType === 's3') {
      innerConfig.bucket = s3Bucket;
      innerConfig.region = s3Region;
      if (s3EndpointUrl) innerConfig.endpoint_url = s3EndpointUrl;
      innerConfig.auth_method = s3AuthMethod;
      if (s3AuthMethod === 'explicit') {
        innerConfig.access_key_id = s3AccessKeyId;
        innerConfig.secret_access_key = {
          type: 'plain',
          value: s3SecretAccessKey,
        };
      }
      innerConfig.path_style_access = s3PathStyleAccess;
    }

    return { type: connectionType, config: innerConfig };
  }

  function handleTestComplete(success: boolean) {
    hasTestedSuccessfully = success;
  }
</script>

<FormPanel {handleSubmit} {handleCancel}>
  <div class="form-group">
    <label for="name">{$t('connections.form.name_label')}</label>
    <input
      id="name"
      type="text"
      bind:value={name}
      placeholder={$t('connections.form.name_placeholder')}
      disabled={mode === 'edit'}
    />
  </div>

  <div class="form-group">
    <fieldset>
      <legend>{$t('connections.form.type_label')}</legend>
      <div class="connection-types">
        <button
          type="button"
          class="type-btn"
          class:active={connectionType === 'sqlite'}
          class:disabled={mode === 'edit'}
          disabled={mode === 'edit'}
          onclick={() => (connectionType = 'sqlite')}
        >
          <Database size={20} />
          <span>{$t('connections.form.sqlite.label')}</span>
        </button>
        <button
          type="button"
          class="type-btn"
          class:active={connectionType === 'localfile'}
          class:disabled={mode === 'edit'}
          disabled={mode === 'edit'}
          onclick={() => (connectionType = 'localfile')}
        >
          <Folder size={20} />
          <span>{$t('connections.form.localfile.label')}</span>
        </button>
        <button
          type="button"
          class="type-btn"
          class:active={connectionType === 'mysql'}
          class:disabled={mode === 'edit'}
          disabled={mode === 'edit'}
          onclick={() => (connectionType = 'mysql')}
        >
          <Database size={20} />
          <span>{$t('connections.form.mysql.label')}</span>
        </button>
        <button
          type="button"
          class="type-btn"
          class:active={connectionType === 'postgresql'}
          class:disabled={mode === 'edit'}
          disabled={mode === 'edit'}
          onclick={() => (connectionType = 'postgresql')}
        >
          <Database size={20} />
          <span>{$t('connections.form.postgresql.label')}</span>
        </button>
        <button
          type="button"
          class="type-btn"
          class:active={connectionType === 's3'}
          class:disabled={mode === 'edit'}
          disabled={mode === 'edit'}
          onclick={() => (connectionType = 's3')}
        >
          <Folder size={20} />
          <span>{$t('connections.form.s3.label')}</span>
        </button>
      </div>
    </fieldset>
  </div>

  {#if connectionType === 'sqlite'}
    <div class="form-group">
      <label for="sqlitePath">{$t('connections.form.sqlite.path_label')}</label>
      <input
        id="sqlitePath"
        type="text"
        bind:value={sqlitePath}
        placeholder={$t('connections.form.sqlite.path_placeholder')}
      />
      <div class="help-text">
        {$t('connections.form.sqlite.help')}
      </div>
    </div>
  {:else if connectionType === 'mysql'}
    <div class="form-group">
      <label for="mysqlHost">{$t('connections.form.mysql.host_label')}</label>
      <input
        id="mysqlHost"
        type="text"
        bind:value={mysqlHost}
        placeholder={$t('connections.form.mysql.host_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="mysqlPort">{$t('connections.form.mysql.port_label')}</label>
      <input
        id="mysqlPort"
        type="number"
        bind:value={mysqlPort}
        placeholder={$t('connections.form.mysql.port_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="mysqlDatabase"
        >{$t('connections.form.mysql.database_label')}</label
      >
      <input
        id="mysqlDatabase"
        type="text"
        bind:value={mysqlDatabase}
        placeholder={$t('connections.form.mysql.database_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="mysqlUsername"
        >{$t('connections.form.mysql.username_label')}</label
      >
      <input
        id="mysqlUsername"
        type="text"
        bind:value={mysqlUsername}
        placeholder={$t('connections.form.mysql.username_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="mysqlPassword"
        >{$t('connections.form.mysql.password_label')}</label
      >
      <input
        id="mysqlPassword"
        type="password"
        bind:value={mysqlPassword}
        placeholder={$t('connections.form.mysql.password_placeholder')}
      />
    </div>
  {:else if connectionType === 'postgresql'}
    <div class="form-group">
      <label for="postgresHost"
        >{$t('connections.form.postgresql.host_label')}</label
      >
      <input
        id="postgresHost"
        type="text"
        bind:value={postgresHost}
        placeholder={$t('connections.form.postgresql.host_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="postgresPort"
        >{$t('connections.form.postgresql.port_label')}</label
      >
      <input
        id="postgresPort"
        type="number"
        bind:value={postgresPort}
        placeholder={$t('connections.form.postgresql.port_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="postgresDatabase"
        >{$t('connections.form.postgresql.database_label')}</label
      >
      <input
        id="postgresDatabase"
        type="text"
        bind:value={postgresDatabase}
        placeholder={$t('connections.form.postgresql.database_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="postgresUsername"
        >{$t('connections.form.postgresql.username_label')}</label
      >
      <input
        id="postgresUsername"
        type="text"
        bind:value={postgresUsername}
        placeholder={$t('connections.form.postgresql.username_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="postgresPassword"
        >{$t('connections.form.postgresql.password_label')}</label
      >
      <input
        id="postgresPassword"
        type="password"
        bind:value={postgresPassword}
        placeholder={$t('connections.form.postgresql.password_placeholder')}
      />
    </div>
  {:else if connectionType === 's3'}
    <div class="form-group">
      <label for="s3Bucket">{$t('connections.form.s3.bucket_label')}</label>
      <input
        id="s3Bucket"
        type="text"
        bind:value={s3Bucket}
        placeholder={$t('connections.form.s3.bucket_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="s3Region">{$t('connections.form.s3.region_label')}</label>
      <input
        id="s3Region"
        type="text"
        bind:value={s3Region}
        placeholder={$t('connections.form.s3.region_placeholder')}
      />
    </div>
    <div class="form-group">
      <label for="s3EndpointUrl"
        >{$t('connections.form.s3.endpoint_label')}</label
      >
      <input
        id="s3EndpointUrl"
        type="text"
        bind:value={s3EndpointUrl}
        placeholder={$t('connections.form.s3.endpoint_placeholder')}
      />
    </div>
    <div class="form-group">
      <fieldset>
        <legend>{$t('connections.form.s3.auth_method_label')}</legend>
        <div class="radio-group">
          <label class="radio-label">
            <input
              type="radio"
              bind:group={s3AuthMethod}
              value="credential_chain"
            />
            {$t('connections.form.s3.auth_credential_chain')}
          </label>
          <label class="radio-label">
            <input type="radio" bind:group={s3AuthMethod} value="explicit" />
            {$t('connections.form.s3.auth_explicit')}
          </label>
        </div>
      </fieldset>
    </div>
    {#if s3AuthMethod === 'explicit'}
      <div class="form-group">
        <label for="s3AccessKeyId"
          >{$t('connections.form.s3.access_key_id_label')}</label
        >
        <input
          id="s3AccessKeyId"
          type="text"
          bind:value={s3AccessKeyId}
          placeholder={$t('connections.form.s3.access_key_id_placeholder')}
        />
      </div>
      <div class="form-group">
        <label for="s3SecretAccessKey"
          >{$t('connections.form.s3.secret_access_key_label')}</label
        >
        <input
          id="s3SecretAccessKey"
          type="password"
          bind:value={s3SecretAccessKey}
          placeholder={$t('connections.form.s3.secret_access_key_placeholder')}
        />
      </div>
    {/if}
    <div class="form-group">
      <label class="checkbox-label">
        <input type="checkbox" bind:checked={s3PathStyleAccess} />
        {$t('connections.form.s3.path_style_access_label')}
      </label>
    </div>
  {:else if connectionType === 'localfile'}
    <div class="form-group">
      <label for="localfilePath"
        >{$t('connections.form.localfile.path_label')}</label
      >
      <input
        id="localfilePath"
        type="text"
        bind:value={localfilePath}
        placeholder={$t('connections.form.localfile.path_placeholder')}
      />
      <div class="help-text">
        {$t('connections.form.localfile.help')}
      </div>
    </div>
  {/if}

  {#snippet actions()}
    <ConnectionTestButton
      connectionConfig={buildConnectionConfig()}
      disabled={!isFormValid()}
      onTestComplete={handleTestComplete}
    />
    <Button
      icon={Save}
      onclick={handleSubmit}
      disabled={!isFormValid() || !hasTestedSuccessfully}
    >
      {$t(
        mode === 'create'
          ? 'connections.form.submit_create'
          : 'connections.form.submit_update',
      )}
    </Button>
  {/snippet}
</FormPanel>

<style>
  .form-group {
    margin-bottom: 20px;
  }

  .form-group label,
  .form-group legend {
    display: block;
    margin-bottom: 6px;
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--color-text-primary);
  }

  .form-group input {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    font-size: 0.95rem;
    box-sizing: border-box;
  }

  .form-group input:focus {
    outline: none;
    border-color: var(--color-primary);
  }

  .form-group input:disabled {
    background: var(--color-background-disabled);
    cursor: not-allowed;
  }

  .connection-types {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 10px;
  }

  .type-btn {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 12px;
    border: 2px solid #ddd;
    border-radius: 8px;
    background: white;
    cursor: pointer;
    transition: all 0.2s;
    gap: 4px;
  }

  .type-btn:hover {
    border-color: var(--color-primary);
    background: var(--color-background-hover);
  }

  .type-btn.active {
    border-color: var(--color-primary);
    background: var(--color-background-selected);
    color: var(--color-primary);
  }

  .type-btn.disabled {
    background: var(--color-background-disabled);
    border-color: var(--color-border-input);
    color: var(--color-text-muted);
    cursor: not-allowed;
  }

  .type-btn.disabled:hover {
    border-color: var(--color-border-input);
    background: var(--color-background-disabled);
  }

  .type-btn span {
    font-size: 0.85rem;
    font-weight: 500;
  }

  fieldset {
    border: none;
    padding: 0;
    margin: 0;
  }

  .radio-group {
    display: flex;
    gap: 20px;
  }

  .radio-label {
    display: flex;
    align-items: center;
    font-weight: normal;
    cursor: pointer;
  }

  .radio-label input {
    width: auto;
    margin-right: 6px;
  }

  .checkbox-label {
    display: flex;
    align-items: center;
    cursor: pointer;
  }

  .checkbox-label input {
    width: auto;
    margin-right: 6px;
  }

  .help-text {
    margin-top: 4px;
    font-size: 0.85rem;
    color: var(--color-text-secondary);
  }
</style>

<script lang="ts">
  import { onMount } from 'svelte';
  import { createEventDispatcher } from 'svelte';
  import type { AdapterConfig, AdapterSource, ColumnInfo } from '../../api';
  import { t } from '../../i18n';
  import { Button } from '../common';
  import { Save, CircleCheckBig, Download, Trash2 } from 'lucide-svelte';
  import { api } from '../../api';
  import FormPanel from '../entity/FormPanel.svelte';

  const dispatch = createEventDispatcher();

  let {
    mode = 'create',
    initialData = null,
  }: {
    mode?: 'create' | 'edit';
    initialData?: { name: string; config: AdapterConfig } | null;
  } = $props();

  let connections = $state<string[]>([]);
  let connectionDetails = $state<Record<string, any>>({});

  onMount(async () => {
    try {
      const connectionList = await api.connections.list();
      connections = connectionList.map((conn) => conn.name);

      // Fetch details for each connection
      for (const conn of connectionList) {
        try {
          const details = await api.connections.get(conn.name);
          connectionDetails[conn.name] = details;
        } catch (error) {
          console.error(
            `Error fetching connection details for ${conn.name}:`,
            error,
          );
        }
      }
    } catch (error) {
      console.error('Error fetching connections:', error);
    }
  });

  let name = $state('');
  let connection = $state('local');
  let description = $state('');
  let sourceType = $state<'file' | 'database'>('file');
  let filePath = $state('');
  let formatType = $state('csv');
  let tableName = $state('');
  let columns = $state<
    Array<{ name: string; type: string; description?: string | null }>
  >([]);
  let isTestingSchema = $state(false);
  let hasTestedSchemaSuccessfully = $state(false);
  let isGettingSchema = $state(false);

  $effect(() => {
    if (initialData && mode === 'edit' && initialData.config) {
      name = initialData.name;
      connection = initialData.config.connection || 'local';
      description = initialData.config.description || '';
      sourceType = initialData.config.source?.type || 'file';
      if (sourceType === 'file' && initialData.config.source?.file) {
        filePath = initialData.config.source.file.path || '';
        formatType = initialData.config.source.format?.type || 'csv';
      } else if (
        sourceType === 'database' &&
        initialData.config.source?.table_name
      ) {
        tableName = initialData.config.source.table_name;
      }
      columns = initialData.config.columns || [];
    }
  });

  // Determine source type based on connection type
  $effect(() => {
    if (connection && connectionDetails[connection]) {
      const connType = connectionDetails[connection].type;
      if (connType === 'localfile' || connType === 's3') {
        sourceType = 'file';
        // Clear database fields
        tableName = '';
      } else if (
        connType === 'sqlite' ||
        connType === 'mysql' ||
        connType === 'postgresql'
      ) {
        sourceType = 'database';
        // Clear file fields
        filePath = '';
        formatType = 'csv';
      }
    }
  });

  // Reset schema test success state when form values change
  $effect(() => {
    // Track all form fields that affect schema validation
    connection;
    sourceType;
    filePath;
    formatType;
    tableName;
    columns;

    // Reset schema test success state when any field changes
    hasTestedSchemaSuccessfully = false;
  });

  function handleSubmit() {
    const config: AdapterConfig = {
      connection,
      description: description || undefined,
      source:
        sourceType === 'file'
          ? {
              type: 'file',
              file: {
                path: filePath,
                compression: null,
                max_batch_size: null,
              },
              format: { type: formatType },
            }
          : {
              type: 'database',
              table_name: tableName,
            },
      columns,
    };

    dispatch('submit', { name, config });
  }

  function handleCancel() {
    name = '';
    connection = 'local';
    description = '';
    sourceType = 'file';
    filePath = '';
    formatType = 'csv';
    tableName = '';
    columns = [];
    dispatch('close');
  }

  function addColumn() {
    columns = [...columns, { name: '', type: '', description: '' }];
  }

  function removeColumn(index: number) {
    columns = columns.filter((_, i) => i !== index);
  }

  function isFormValid() {
    return name && (sourceType === 'file' ? filePath : tableName);
  }

  async function handleTestSchema() {
    if (!connection || !columns.length || (!filePath && !tableName)) {
      window.showToast?.error(
        $t('adapters.form.schema_test_error', {
          values: { error: 'Invalid configuration' },
        }),
      );
      return;
    }

    isTestingSchema = true;
    try {
      const testRequest = {
        connection,
        source:
          sourceType === 'file'
            ? {
                type: 'file' as const,
                file: {
                  path: filePath,
                  compression: null,
                  max_batch_size: null,
                },
                format: { type: formatType },
              }
            : {
                type: 'database' as const,
                table_name: tableName,
              },
        columns: columns
          .filter((col) => col.name && col.type)
          .map((col) => ({
            name: col.name,
            type: col.type,
            description: col.description || undefined,
          })),
      };

      await api.adapters.testSchema(testRequest);
      hasTestedSchemaSuccessfully = true;
      window.showToast?.success($t('adapters.form.schema_test_success'));
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      window.showToast?.error(
        $t('adapters.form.schema_test_error', {
          values: { error: errorMessage },
        }),
      );
    } finally {
      isTestingSchema = false;
    }
  }

  async function handleGetSchema() {
    if (!connection || (!filePath && !tableName)) {
      window.showToast?.error(
        $t('adapters.form.get_schema_error', {
          values: { error: 'Invalid configuration' },
        }),
      );
      return;
    }

    isGettingSchema = true;
    try {
      const schemaRequest = {
        connection,
        source:
          sourceType === 'file'
            ? {
                type: 'file' as const,
                file: {
                  path: filePath,
                  compression: null,
                  max_batch_size: null,
                },
                format: { type: formatType },
              }
            : {
                type: 'database' as const,
                table_name: tableName,
              },
      };

      const schemaInfo = await api.adapters.getSchema(schemaRequest);

      if (columns.length > 0) {
        const confirmed = window.confirm(
          $t('adapters.form.replace_columns_confirm'),
        );
        if (!confirmed) {
          return;
        }
      }

      columns = schemaInfo.map((col: ColumnInfo) => ({
        name: col.name,
        type: col.data_type,
        description: '',
      }));

      window.showToast?.success(
        $t('adapters.form.get_schema_success', {
          values: { count: schemaInfo.length },
        }),
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      window.showToast?.error(
        $t('adapters.form.get_schema_error', {
          values: { error: errorMessage },
        }),
      );
    } finally {
      isGettingSchema = false;
    }
  }
</script>

<FormPanel {handleSubmit} {handleCancel}>
  <div class="form-group">
    <label for="name">{$t('adapters.form.name_label')}</label>
    <input
      id="name"
      type="text"
      bind:value={name}
      placeholder={$t('adapters.form.name_placeholder')}
      disabled={mode === 'edit'}
    />
  </div>

  <div class="form-group">
    <label for="connection">{$t('adapters.form.connection_label')}</label>
    <select id="connection" bind:value={connection}>
      {#each connections as conn}
        <option value={conn}>{conn}</option>
      {/each}
    </select>
  </div>

  <div class="form-group">
    <label for="description">{$t('adapters.form.description_label')}</label>
    <textarea
      id="description"
      bind:value={description}
      placeholder={$t('adapters.form.description_placeholder')}
      rows="3"
    ></textarea>
  </div>

  {#if connection && connectionDetails[connection]}
    {@const connType = connectionDetails[connection].type}
    <div class="form-group">
      <fieldset>
        <legend>{$t('adapters.form.source_type_label')}</legend>
        <div class="source-type-display">
          {#if connType === 'localfile' || connType === 's3'}
            <span class="source-type-info"
              >{$t('adapters.form.source_file')} ({connType})</span
            >
          {:else if connType === 'sqlite' || connType === 'mysql' || connType === 'postgresql'}
            <span class="source-type-info"
              >{$t('adapters.form.source_database')} ({connType})</span
            >
          {/if}
        </div>
      </fieldset>
    </div>
  {/if}

  {#if sourceType === 'file'}
    <div class="form-group">
      <label for="filePath">{$t('adapters.form.file_path_label')}</label>
      <input
        id="filePath"
        type="text"
        bind:value={filePath}
        placeholder={$t('adapters.form.file_path_placeholder')}
      />
    </div>

    <div class="form-group">
      <label for="formatType">{$t('adapters.form.format_label')}</label>
      <select id="formatType" bind:value={formatType}>
        <option value="csv">{$t('adapters.format_options.csv')}</option>
        <option value="json">{$t('adapters.format_options.json')}</option>
        <option value="parquet">{$t('adapters.format_options.parquet')}</option>
      </select>
    </div>
  {:else}
    <div class="form-group">
      <label for="tableName">{$t('adapters.form.table_name_label')}</label>
      <input
        id="tableName"
        type="text"
        bind:value={tableName}
        placeholder={$t('adapters.form.table_name_placeholder')}
      />
    </div>
  {/if}

  <!-- Schema Section -->
  <div class="form-group">
    <fieldset>
      <legend>{$t('adapters.form.schema_label')}</legend>
      <div class="schema-section">
        {#if columns.length === 0}
          <p class="no-columns">{$t('adapters.form.no_columns')}</p>
        {:else}
          <div class="columns-list">
            {#each columns as column, index}
              <div class="column-row">
                <input
                  type="text"
                  bind:value={column.name}
                  placeholder={$t('adapters.form.column_name_placeholder')}
                  class="column-name"
                />
                <select bind:value={column.type} class="column-type">
                  <option value="">{$t('adapters.form.select_type')}</option>
                  <option value="BIGINT">BIGINT</option>
                  <option value="BOOLEAN">BOOLEAN</option>
                  <option value="DATE">DATE</option>
                  <option value="DATETIME">DATETIME</option>
                  <option value="DOUBLE">DOUBLE</option>
                  <option value="FLOAT">FLOAT</option>
                  <option value="INTEGER">INTEGER</option>
                  <option value="REAL">REAL</option>
                  <option value="TEXT">TEXT</option>
                  <option value="TIMESTAMP">TIMESTAMP</option>
                  <option value="VARCHAR">VARCHAR</option>
                </select>
                <input
                  type="text"
                  bind:value={column.description}
                  placeholder={$t(
                    'adapters.form.column_description_placeholder',
                  )}
                  class="column-description"
                />
                <button
                  type="button"
                  onclick={() => removeColumn(index)}
                  class="remove-column-btn"
                  aria-label={$t('adapters.form.remove_column')}
                >
                  <Trash2 size="14" />
                </button>
              </div>
            {/each}
          </div>
        {/if}
        <div class="schema-buttons">
          <button type="button" onclick={addColumn} class="add-column-btn">
            {$t('adapters.form.add_column')}
          </button>
          <button
            type="button"
            onclick={handleGetSchema}
            class="get-schema-btn"
            disabled={isGettingSchema ||
              !connection ||
              (!filePath && !tableName)}
          >
            {$t(
              isGettingSchema
                ? 'adapters.form.getting_schema'
                : 'adapters.form.get_schema',
            )}
          </button>
        </div>
      </div>
    </fieldset>
  </div>

  {#snippet actions()}
    <Button
      icon={CircleCheckBig}
      onclick={handleTestSchema}
      disabled={isTestingSchema || !connection || (!filePath && !tableName)}
    >
      {$t(
        isTestingSchema
          ? 'adapters.form.testing_schema'
          : 'adapters.form.test_schema',
      )}
    </Button>
    <Button
      icon={Save}
      onclick={handleSubmit}
      disabled={!isFormValid() || !hasTestedSchemaSuccessfully}
    >
      {$t(
        mode === 'create'
          ? 'adapters.form.submit_create'
          : 'adapters.form.submit_update',
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

  .form-group input,
  .form-group textarea,
  .form-group select {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    font-size: 0.95rem;
    box-sizing: border-box;
  }

  .form-group input:focus,
  .form-group textarea:focus,
  .form-group select:focus {
    outline: none;
    border-color: var(--color-primary);
  }

  .form-group input:disabled {
    background: var(--color-background-disabled);
    cursor: not-allowed;
  }

  .form-group textarea {
    resize: vertical;
  }

  fieldset {
    border: none;
    padding: 0;
    margin: 0;
  }

  .schema-section {
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    padding: 16px;
    background: var(--color-background-secondary, #fafafa);
  }

  .no-columns {
    color: var(--color-text-secondary);
    font-style: italic;
    margin: 0 0 12px 0;
  }

  .columns-list {
    margin-bottom: 12px;
  }

  .column-row {
    display: grid;
    grid-template-columns: 1fr 120px 1fr 32px;
    gap: 8px;
    margin-bottom: 8px;
    align-items: center;
  }

  .column-name,
  .column-type,
  .column-description {
    padding: 6px 8px;
    border: 1px solid var(--color-border-input);
    border-radius: 3px;
    font-size: 0.9rem;
  }

  .column-type {
    font-size: 0.85rem;
  }

  .remove-column-btn {
    width: 28px;
    height: 28px;
    border: 1px solid var(--color-border-danger, #dc3545);
    background: var(--color-background-danger, #f8d7da);
    color: var(--color-text-danger, #721c24);
    border-radius: 4px;
    cursor: pointer;
    font-size: 16px;
    font-weight: bold;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .remove-column-btn:hover {
    background: var(--color-background-danger-hover, #f1aeb5);
  }

  .add-column-btn {
    padding: 8px 16px;
    border: 1px solid var(--color-primary);
    background: transparent;
    color: var(--color-primary);
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
    transition: all 0.2s ease;
  }

  .add-column-btn:hover {
    background: var(--color-primary);
    color: white;
  }

  .schema-buttons {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
  }

  .get-schema-btn {
    padding: 8px 16px;
    border: 1px solid var(--color-secondary, #6c757d);
    background: transparent;
    color: var(--color-secondary, #6c757d);
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .get-schema-btn:hover:not(:disabled) {
    background: var(--color-secondary, #6c757d);
    color: white;
  }

  .get-schema-btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .source-type-display {
    padding: 8px 12px;
    background: var(--color-background-secondary, #f5f5f5);
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
  }

  .source-type-info {
    color: var(--color-text-primary);
    font-weight: 500;
    font-size: 0.95rem;
  }
</style>

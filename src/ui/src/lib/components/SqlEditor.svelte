<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { Play } from 'lucide-svelte';
  import { t } from '../i18n';
  import { api } from '../api';
  import { Button } from './common';
  import ErrorMessage from '../components/ErrorMessage.svelte';

  const dispatch = createEventDispatcher();

  let {
    sql = $bindable(''),
    placeholder = '',
    rows = 10,
    readonly = false,
    enableExecution = false,
    context = 'query',
  }: {
    sql?: string;
    placeholder?: string;
    rows?: number;
    readonly?: boolean;
    enableExecution?: boolean;
    context?: 'query' | 'model';
  } = $props();

  let loading = $state(false);
  const textareaId = `sql-textarea-${Math.random().toString(36).substr(2, 9)}`;
  let results = $state<any | null>(null);
  let error = $state<string | null>(null);

  function handleSqlChange(event: Event) {
    const target = event.target as HTMLTextAreaElement;
    sql = target.value;
    dispatch('sqlChange', sql);
  }

  async function executeQuery() {
    if (!sql.trim()) return;

    loading = true;
    error = null;
    results = null;

    try {
      const data = await api.queries.execute(sql.trim());
      results = data;
    } catch (e) {
      if (e instanceof Error) {
        error = e.message;
      } else {
        error = $t('query.error');
      }
    } finally {
      loading = false;
    }
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.ctrlKey && event.key === 'Enter' && enableExecution) {
      event.preventDefault();
      executeQuery();
    }
  }
</script>

<div class="sql-editor-container">
  <div class="editor-section">
    <div class="editor-header">
      {#if enableExecution}
        <div class="editor-actions">
          <Button
            icon={Play}
            onclick={executeQuery}
            disabled={loading || !sql.trim()}
          >
            {loading ? $t('query.executing') : $t('query.execute')}
          </Button>
        </div>
      {/if}
    </div>

    {#if readonly}
      <pre class="sql-display">{sql}</pre>
    {:else}
      <textarea
        id={textareaId}
        class="sql-textarea"
        bind:value={sql}
        oninput={handleSqlChange}
        onkeydown={handleKeydown}
        {placeholder}
        {rows}
        {readonly}
      ></textarea>
    {/if}

    {#if enableExecution}
      <div class="execution-hint">{$t('query.editor.execution_hint')}</div>
    {/if}
  </div>

  {#if enableExecution && (results || error)}
    <div class="results-section">
      {#if error}
        <ErrorMessage message={error} monospace={true} />
      {:else if results}
        <div class="results">
          <h4>
            {$t('query.editor.results_title', {
              values: {
                rows: results.row_count || 0,
                columns: results.column_count || 0,
              },
            })}
          </h4>
          {#if results.data && results.row_count === 0}
            <p class="no-results">{$t('query.editor.no_results_found')}</p>
          {:else if results.data}
            <div class="table-container">
              <table>
                <thead>
                  <tr>
                    {#each Object.keys(results.data) as columnName}
                      <th>{columnName}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each Array(results.row_count) as _, rowIndex}
                    <tr>
                      {#each Object.keys(results.data) as columnName}
                        <td>{results.data[columnName][rowIndex] || ''}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .sql-editor-container {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  .editor-section {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .editor-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .editor-actions {
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: flex-end;
  }

  .sql-textarea {
    width: 100%;
    padding: 12px;
    border: 1px solid var(--color-border-input);
    border-radius: 4px;
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: var(--font-small-size);
    line-height: var(--font-small-line-height);
    resize: vertical;
  }

  .sql-textarea:focus {
    outline: none;
    border-color: var(--color-primary);
  }

  .sql-display {
    padding: 12px;
    background: var(--color-background-code);
    border: 1px solid var(--color-border-lighter);
    border-radius: 4px;
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: var(--font-small-size);
    line-height: var(--font-small-line-height);
    margin: 0;
    overflow-x: auto;
    white-space: pre-wrap;
  }

  .execution-hint {
    font-size: var(--font-caption-size);
    font-weight: var(--font-caption-weight);
    line-height: var(--font-caption-line-height);
    color: var(--color-text-secondary);
    font-style: italic;
  }

  .results-section {
    border: 1px solid var(--color-border);
    border-radius: 4px;
    background: white;
  }

  .results {
    padding: 16px;
  }

  .results h4 {
    margin: 0 0 16px 0;
    font-size: var(--font-h4-size);
    font-weight: var(--font-h4-weight);
    line-height: var(--font-h4-line-height);
    color: var(--color-text-slate);
  }

  .no-results {
    font-size: var(--font-body-size);
    font-weight: var(--font-body-weight);
    line-height: var(--font-body-line-height);
    color: var(--color-text-light);
    font-style: italic;
  }

  .table-container {
    overflow-x: auto;
    border: 1px solid var(--color-border-lighter);
    border-radius: 4px;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
  }

  th {
    background: var(--color-background-light);
    padding: 8px 12px;
    text-align: left;
    font-weight: var(--font-label-weight);
    color: var(--color-text-slate);
    border-bottom: 2px solid var(--color-border-lighter);
  }

  td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--color-border-lighter);
  }

  tbody tr:hover {
    background: #f9fafb;
  }
</style>

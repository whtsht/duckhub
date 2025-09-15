<script lang="ts">
  import { onMount } from 'svelte';
  import {
    Play,
    Database,
    AlertCircle,
    CheckCircle,
    Loader2,
  } from 'lucide-svelte';
  import { t } from '../i18n';
  import PipelineGraph from '../PipelineGraph.svelte';
  import {
    api,
    type GraphData,
    type Pipeline,
    type TaskStatus,
    type GraphNode,
    type GraphEdge,
  } from '../api';
  import { Button, RefreshButton } from '../components/common';

  let error = $state<string | null>(null);
  let loading = $state(false);
  let nodes = $state<GraphNode[]>([]);
  let edges = $state<GraphEdge[]>([]);
  let pipelineStatus = $state<Pipeline | null>(null);
  let selectedNode = $state<string | null>(null);

  onMount(() => {
    refreshGraph();
  });

  async function refreshGraph() {
    try {
      loading = true;
      error = null;

      const data = await api.pipeline.getGraph();
      nodes = Object.values(data.nodes || {});

      edges = [];
      for (const [nodeName, nodeData] of Object.entries(data.nodes || {})) {
        for (const dependency of nodeData.dependencies || []) {
          edges.push({
            from: dependency,
            to: nodeName,
          });
        }
      }

      pipelineStatus = await api.pipeline.getStatus();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to refresh graph';
    } finally {
      loading = false;
    }
  }

  async function loadGraph() {
    try {
      error = null;

      const data = await api.pipeline.getGraph();
      nodes = Object.values(data.nodes || {});

      edges = [];
      for (const [nodeName, nodeData] of Object.entries(data.nodes || {})) {
        for (const dependency of nodeData.dependencies || []) {
          edges.push({
            from: dependency,
            to: nodeName,
          });
        }
      }

      pipelineStatus = await api.pipeline.getStatus();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Unknown error';
    }
  }

  async function runPipeline() {
    try {
      error = null;
      await api.pipeline.run();
      window.showToast?.success($t('pipeline.run_success'));
    } catch (e) {
      error = e instanceof Error ? e.message : 'Pipeline execution failed';
    }
  }

  async function runNode(nodeName: string) {
    try {
      error = null;

      await api.pipeline.runNode(nodeName);
      window.showToast?.success(
        $t('pipeline.node_run_success', { values: { node: nodeName } }),
      );
    } catch (e) {
      error = e instanceof Error ? e.message : `Failed to run node ${nodeName}`;
    }
  }

  function onNodeClick(nodeName: string) {
    selectedNode = selectedNode === nodeName ? null : nodeName;
  }

  function getSelectedNodeInfo() {
    if (!selectedNode) return null;
    const node = nodes.find((n) => n.name === selectedNode);
    const taskStatus = pipelineStatus?.tasks?.[selectedNode];
    return { node, taskStatus };
  }

  function getStatusIcon(status: string) {
    switch (status.toLowerCase()) {
      case 'running':
        return Loader2;
      case 'completed':
        return CheckCircle;
      case 'failed':
        return AlertCircle;
      default:
        return Database;
    }
  }

  function getStatusColor(status: string) {
    switch (status.toLowerCase()) {
      case 'running':
        return 'text-orange-500';
      case 'completed':
        return 'text-green-500';
      case 'failed':
        return 'text-red-500';
      default:
        return 'text-gray-500';
    }
  }
</script>

<div class="pipeline-panel">
  <div class="header">
    <h2 class="title">{$t('graph.title')}</h2>

    <div class="actions">
      <RefreshButton onRefresh={refreshGraph} {loading} />
      <Button
        icon={Play}
        onclick={runPipeline}
        disabled={nodes.length === 0 ||
          pipelineStatus?.phase === 'running' ||
          loading}
      >
        {$t('graph.run')}
      </Button>
    </div>
  </div>

  {#if error}
    <div class="error">
      <AlertCircle size={16} />
      {error}
    </div>
  {/if}

  {#if pipelineStatus}
    <div class="status-bar">
      <div class="status-item">
        {#if pipelineStatus}
          {@const StatusIcon = getStatusIcon(pipelineStatus.phase)}
          <StatusIcon size={16} class={getStatusColor(pipelineStatus.phase)} />
        {/if}
        <span
          >{$t('graph.status_labels.pipeline_status', {
            values: {
              status: $t(
                `pipeline.phase.${pipelineStatus.phase.toLowerCase()}`,
              ),
            },
          })}</span
        >
        {#if pipelineStatus.phase === 'running'}
          <span class="text-sm text-gray-500">
            {$t('graph.status_labels.completed_tasks', {
              values: {
                completed: Object.values(pipelineStatus.tasks).filter(
                  (t) => t.phase === 'completed',
                ).length,
                total: Object.keys(pipelineStatus.tasks).length,
              },
            })}
          </span>
        {/if}
      </div>

      {#if pipelineStatus.started_at}
        <div class="status-item text-sm text-gray-500">
          {$t('graph.status_labels.started_at', {
            values: {
              time: new Date(pipelineStatus.started_at).toLocaleString(),
            },
          })}
        </div>
      {/if}
    </div>
  {/if}

  <div class="graph-section">
    {#if nodes.length === 0}
      <div class="empty-state">
        <Database size={48} class="text-gray-400" />
        <h3>{$t('graph.empty_state.title')}</h3>
        <p>{$t('graph.empty_state.description')}</p>
      </div>
    {:else}
      <div class="graph-layout">
        <div class="graph-container">
          <PipelineGraph {nodes} {edges} {pipelineStatus} {onNodeClick} />
        </div>

        {#if selectedNode}
          {@const nodeInfo = getSelectedNodeInfo()}
          {#if nodeInfo}
            <div class="node-panel">
              <div class="node-panel-header">
                <h3>{selectedNode}</h3>
                <button
                  onclick={() => (selectedNode = null)}
                  class="close-button"
                >
                  ×
                </button>
              </div>

              <dl class="node-details">
                {#if nodeInfo.node?.updated_at}
                  <div class="detail-item">
                    <dt>{$t('graph.status_labels.last_updated')}</dt>
                    <dd>
                      {new Date(nodeInfo.node.updated_at).toLocaleString()}
                    </dd>
                  </div>
                {/if}

                {#if nodeInfo.taskStatus}
                  <div class="detail-item">
                    <dt>{$t('graph.status_labels.status')}</dt>
                    <dd
                      class="value status-{nodeInfo.taskStatus.phase.toLowerCase()}"
                    >
                      {$t(
                        `pipeline.phase.${nodeInfo.taskStatus.phase.toLowerCase()}`,
                      )}
                    </dd>
                  </div>

                  {#if nodeInfo.taskStatus.started_at}
                    <div class="detail-item">
                      <dt>{$t('graph.status_labels.started')}</dt>
                      <dd>
                        {new Date(
                          nodeInfo.taskStatus.started_at,
                        ).toLocaleString()}
                      </dd>
                    </div>
                  {/if}

                  {#if nodeInfo.taskStatus.completed_at}
                    <div class="detail-item">
                      <dt>{$t('graph.status_labels.completed')}</dt>
                      <dd>
                        {new Date(
                          nodeInfo.taskStatus.completed_at,
                        ).toLocaleString()}
                      </dd>
                    </div>
                  {/if}
                {/if}
              </dl>

              <div class="node-actions">
                <Button
                  icon={Play}
                  onclick={() => runNode(selectedNode!)}
                  disabled={pipelineStatus?.phase === 'running' || loading}
                >
                  {$t('graph.run_node')}
                </Button>
              </div>
            </div>
          {/if}
        {/if}
      </div>
    {/if}
  </div>
</div>

<style>
  .pipeline-panel {
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 1rem;
    gap: 1rem;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid #e0e0e0;
    padding-bottom: 1rem;
  }

  .title {
    font-size: var(--font-h2-size);
    font-weight: var(--font-h2-weight);
    line-height: var(--font-h2-line-height);
    margin: 0;
  }

  .actions {
    display: flex;
    gap: 0.5rem;
  }

  .btn {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 0.375rem;
    border: 1px solid transparent;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .error {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem 1rem;
    background-color: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 0.375rem;
    color: #dc2626;
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
  }

  .status-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.75rem 1rem;
    background-color: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 0.375rem;
  }

  .status-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .graph-section {
    flex: 1;
    display: flex;
    flex-direction: column;
  }

  .graph-layout {
    flex: 1;
    display: flex;
    gap: 1rem;
  }

  .graph-container {
    flex: 1;
    min-width: 0;
  }

  .node-panel {
    width: 300px;
    flex-shrink: 0;
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 0.5rem;
    display: flex;
    flex-direction: column;
  }

  .node-panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1rem;
    border-bottom: 1px solid #e2e8f0;
    background: #f8fafc;
    border-radius: 0.5rem 0.5rem 0 0;
  }

  .node-panel-header h3 {
    margin: 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
    color: #1e293b;
  }

  .close-button {
    background: none;
    border: none;
    font-size: 1.5rem;
    cursor: pointer;
    color: #64748b;
    padding: 0;
    width: 24px;
    height: 24px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 0.25rem;
  }

  .close-button:hover {
    background: #e2e8f0;
    color: #1e293b;
  }

  .node-details {
    flex: 1;
    padding: 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .detail-item {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .detail-item dt {
    font-size: var(--font-caption-size);
    font-weight: var(--font-label-weight);
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .detail-item dd {
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
    color: #1e293b;
  }

  .status-running {
    color: #ea580c !important;
    font-weight: 600;
  }

  .status-completed {
    color: #16a34a !important;
    font-weight: 600;
  }

  .status-failed {
    color: #dc2626 !important;
    font-weight: 600;
  }

  .status-waiting {
    color: #64748b !important;
  }

  .node-actions {
    padding: 1rem;
    border-top: 1px solid #e2e8f0;
    background: #f8fafc;
    border-radius: 0 0 0.5rem 0.5rem;
  }

  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    flex: 1;
    gap: 1rem;
    text-align: center;
    color: #6b7280;
  }

  .empty-state h3 {
    margin: 0;
    font-size: var(--font-h3-size);
    font-weight: var(--font-h3-weight);
    line-height: var(--font-h3-line-height);
  }

  .empty-state p {
    margin: 0;
    font-size: var(--font-small-size);
    font-weight: var(--font-small-weight);
    line-height: var(--font-small-line-height);
  }

  :global(.animate-spin) {
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }
</style>

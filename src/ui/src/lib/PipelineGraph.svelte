<script lang="ts">
  import { onMount } from 'svelte';
  import cytoscape from 'cytoscape';
  import dagre from 'cytoscape-dagre';
  import type { GraphNode, GraphEdge, Pipeline } from './api';

  type Props = {
    nodes: GraphNode[];
    edges: GraphEdge[];
    pipelineStatus?: Pipeline | null;
    onNodeClick?: (nodeName: string) => void;
  };

  let { nodes, edges, pipelineStatus, onNodeClick }: Props = $props();

  function getNodeStatus(nodeName: string): string {
    if (!pipelineStatus?.tasks) return 'waiting';
    return pipelineStatus.tasks[nodeName]?.phase.toLowerCase() || 'waiting';
  }

  let container: HTMLDivElement;
  let cy: cytoscape.Core | null = null;

  onMount(() => {
    cytoscape.use(dagre);
    initializeGraph();
    return () => {
      if (cy) {
        cy.destroy();
      }
    };
  });

  function initializeGraph() {
    if (!container) return;

    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.name,
          label: node.name,
          status: getNodeStatus(node.name),
          lastUpdated: node.updated_at,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: `${edge.from}-${edge.to}`,
          source: edge.from,
          target: edge.to,
        },
      })),
    ];

    cy = cytoscape({
      container,
      elements,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#e9ecef',
            'border-color': '#333',
            'border-width': '1px',
            label: (ele: any) => {
              const name = ele.data('label');
              const status = ele.data('status');
              const icon = getStatusIcon(status);
              return `${icon} ${name}`;
            },
            'text-valign': 'center',
            'text-halign': 'center',
            color: '#333',
            'font-size': '12px',
            width: 'label',
            height: 'label',
            'text-wrap': 'wrap',
            'text-max-width': '200px',
            padding: '12px',
            shape: 'roundrectangle',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#666',
            'target-arrow-color': '#666',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
          },
        },
        {
          selector: 'node:selected',
          style: {
            'background-color': '#e8f5e9',
            'border-color': '#27ae60',
            'border-width': '2px',
          },
        },
      ],
      layout: {
        name: 'dagre',
        rankDir: 'TB',
        padding: 10,
      } as any,
    });

    cy.on('tap', 'node', (event: cytoscape.EventObject) => {
      const nodeName = event.target.data('id');
      onNodeClick?.(nodeName);
    });
  }

  function getStatusIcon(status: string) {
    switch (status) {
      case 'running':
        return '⚡';
      case 'completed':
        return '✓';
      case 'failed':
        return '✗';
      case 'waiting':
        return '⏸';
      default:
        return '⏸';
    }
  }

  $effect(() => {
    if (cy && nodes && edges) {
      updateGraph();
    }
  });

  function updateGraph() {
    if (!cy) return;

    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.name,
          label: node.name,
          status: getNodeStatus(node.name),
          lastUpdated: node.updated_at,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: `${edge.from}-${edge.to}`,
          source: edge.from,
          target: edge.to,
        },
      })),
    ];

    cy.json({ elements });
    cy.layout({
      name: 'dagre',
      rankDir: 'TB',
      padding: 10,
    } as any).run();
  }
</script>

<div class="graph-container">
  <div bind:this={container} class="cytoscape-container"></div>
</div>

<style>
  .graph-container {
    width: 100%;
    height: 75vh;
    position: relative;
  }

  .cytoscape-container {
    width: 100%;
    height: 100%;
    border: 1px solid var(--color-border);
    border-radius: 4px;
  }
</style>

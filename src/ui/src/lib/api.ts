import { z } from 'zod';
const API_BASE_URL = 'http://localhost:3015';

const AdapterSummarySchema = z.object({
  name: z.string(),
  description: z.string().nullable().optional(),
  connection: z.string(),
  source_type: z.string(),
});

const FileConfigSchema = z.object({
  path: z.string(),
  compression: z.string().nullable().optional(),
  max_batch_size: z.string().nullable().optional(),
});

const FormatConfigSchema = z.object({
  type: z.string(),
  delimiter: z.string().nullable().optional(),
  null_value: z.string().nullable().optional(),
  has_header: z.boolean().nullable().optional(),
});

const AdapterSourceSchema = z.object({
  type: z.enum(['file', 'database']),
  file: FileConfigSchema.optional(),
  format: FormatConfigSchema.optional(),
  table_name: z.string().optional(),
});

const ColumnConfigSchema = z.object({
  name: z.string(),
  type: z.string(),
  description: z.string().nullable().optional(),
});

const ColumnInfoSchema = z.object({
  name: z.string(),
  data_type: z.string(),
});

const AdapterConfigSchema = z.object({
  connection: z.string(),
  description: z.string().nullable().optional(),
  source: AdapterSourceSchema,
  columns: z.array(ColumnConfigSchema),
});

const ModelSummarySchema = z.object({
  name: z.string(),
  description: z.string().nullable().optional(),
});

const ModelConfigSchema = z.object({
  sql: z.string(),
  description: z.string().nullable().optional(),
  depends: z.array(z.string()).optional(),
});

const ConnectionSummarySchema = z.object({
  name: z.string(),
  connection_type: z.string(),
  details: z.string(),
});

const LocalFileConnectionConfigSchema = z.object({
  base_path: z.string(),
});

const SqliteConnectionConfigSchema = z.object({
  path: z.string(),
});

const MySqlConnectionConfigSchema = z.object({
  host: z.string(),
  port: z.number(),
  database: z.string(),
  username: z.string(),
  password: z.union([
    z.object({ type: z.literal('plain'), value: z.string() }),
    z.object({ type: z.literal('encrypted'), value: z.string() }),
  ]),
});

const PostgreSqlConnectionConfigSchema = z.object({
  host: z.string(),
  port: z.number(),
  database: z.string(),
  username: z.string(),
  password: z.union([
    z.object({ type: z.literal('plain'), value: z.string() }),
    z.object({ type: z.literal('encrypted'), value: z.string() }),
  ]),
});

const S3ConnectionConfigSchema = z.object({
  bucket: z.string(),
  region: z.string(),
  endpoint_url: z.string().nullable().optional(),
  auth_method: z.enum(['credential_chain', 'explicit']),
  access_key_id: z.string().nullable().optional(),
  secret_access_key: z
    .union([
      z.object({ type: z.literal('plain'), value: z.string() }),
      z.object({ type: z.literal('encrypted'), value: z.string() }),
    ])
    .nullable()
    .optional(),
  path_style_access: z.boolean(),
});

const LocalFileConnectionSchema = z.object({
  type: z.literal('localfile'),
  config: LocalFileConnectionConfigSchema,
});

const SqliteConnectionSchema = z.object({
  type: z.literal('sqlite'),
  config: SqliteConnectionConfigSchema,
});

const MySqlConnectionSchema = z.object({
  type: z.literal('mysql'),
  config: MySqlConnectionConfigSchema,
});

const PostgreSqlConnectionSchema = z.object({
  type: z.literal('postgresql'),
  config: PostgreSqlConnectionConfigSchema,
});

const S3ConnectionSchema = z.object({
  type: z.literal('s3'),
  config: S3ConnectionConfigSchema,
});

const ConnectionConfigSchema = z.discriminatedUnion('type', [
  LocalFileConnectionSchema,
  SqliteConnectionSchema,
  MySqlConnectionSchema,
  PostgreSqlConnectionSchema,
  S3ConnectionSchema,
]);

const QuerySummarySchema = z.object({
  name: z.string(),
  description: z.string().nullable().optional(),
});

const QueryConfigSchema = z.object({
  description: z.string().nullable().optional(),
  sql: z.string(),
});

// Connection test API用の型定義
export type TestConnectionConfig =
  | { type: 'sqlite'; path: string }
  | { type: 'localfile'; base_path: string }
  | {
      type: 'mysql';
      host: string;
      port: number;
      database: string;
      username: string;
      password: string;
    }
  | {
      type: 'postgresql';
      host: string;
      port: number;
      database: string;
      username: string;
      password: string;
    }
  | {
      type: 's3';
      bucket: string;
      region: string;
      endpoint_url?: string;
      auth_method: string;
      access_key_id?: string;
      secret_access_key?: string;
      path_style_access: boolean;
    };

const DashboardSummarySchema = z.object({
  name: z.string(),
  description: z.string().nullable().optional(),
  query_name: z.string().optional(),
  chart_type: z.string().optional(),
});

const DashboardConfigSchema = z.object({
  description: z.string().nullable().optional(),
  query: z.string(),
  chart: z.object({
    type: z.enum(['line', 'bar']),
    x_column: z.string(),
    y_column: z.string(),
  }),
});

const TaskStatusSchema = z.object({
  phase: z.string(),
  started_at: z.string().nullable().optional(),
  completed_at: z.string().nullable().optional(),
  error: z
    .object({
      message: z.string(),
      at: z.string(),
    })
    .nullable()
    .optional(),
});

const PipelineSchema = z
  .object({
    phase: z.string(),
    started_at: z.string().nullable().optional(),
    completed_at: z.string().nullable().optional(),
    tasks: z.record(z.string(), TaskStatusSchema),
  })
  .nullable();

const GraphNodeSchema = z.object({
  name: z.string(),
  updated_at: z.string().nullable(),
  dependencies: z.array(z.string()),
});

const GraphDataSchema = z.object({
  nodes: z.record(z.string(), GraphNodeSchema),
});

export type GraphNode = z.infer<typeof GraphNodeSchema>;
export type GraphEdge = {
  from: string;
  to: string;
};

export type AdapterSummary = z.infer<typeof AdapterSummarySchema>;
export type AdapterConfig = z.infer<typeof AdapterConfigSchema>;
export type AdapterSource = z.infer<typeof AdapterSourceSchema>;
export type ColumnInfo = z.infer<typeof ColumnInfoSchema>;

export type ModelSummary = z.infer<typeof ModelSummarySchema>;
export type ModelConfig = z.infer<typeof ModelConfigSchema>;

export type ConnectionSummary = z.infer<typeof ConnectionSummarySchema>;
export type LocalFileConnection = z.infer<typeof LocalFileConnectionSchema>;
export type SqliteConnection = z.infer<typeof SqliteConnectionSchema>;
export type MySqlConnection = z.infer<typeof MySqlConnectionSchema>;
export type PostgreSqlConnection = z.infer<typeof PostgreSqlConnectionSchema>;
export type S3Connection = z.infer<typeof S3ConnectionSchema>;
export type ConnectionConfig = z.infer<typeof ConnectionConfigSchema>;

export type QuerySummary = z.infer<typeof QuerySummarySchema>;
export type QueryConfig = z.infer<typeof QueryConfigSchema>;

export type DashboardSummary = z.infer<typeof DashboardSummarySchema>;
export type DashboardConfig = z.infer<typeof DashboardConfigSchema>;

export type Pipeline = z.infer<typeof PipelineSchema>;
export type TaskStatus = z.infer<typeof TaskStatusSchema>;
export type GraphData = z.infer<typeof GraphDataSchema>;

export class ApiError extends Error {
  constructor(message: string) {
    super(message);
  }
}

async function apiRequest(
  endpoint: string,
  options: RequestInit = {},
): Promise<Response> {
  const url = `${API_BASE_URL}${endpoint}`;
  const response = await fetch(url, options);

  if (!response.ok) {
    let errorMessage = `HTTP ${response.status}: ${response.statusText}`;

    try {
      const contentType = response.headers.get('content-type');
      if (contentType && contentType.includes('application/json')) {
        const error = await response.json();
        errorMessage = error.message || errorMessage;
      } else {
        const text = await response.text();
        errorMessage = text || errorMessage;
      }
    } catch (parseError) {
      console.warn('Failed to parse error response:', parseError);
    }

    throw new ApiError(errorMessage);
  }

  return response;
}

const adapters = {
  async list(): Promise<AdapterSummary[]> {
    const response = await apiRequest('/api/adapters');
    const data = await response.json();
    try {
      return z.array(AdapterSummarySchema).parse(data);
    } catch (error) {
      console.error('Zod validation error for adapters:', error, 'Data:', data);
      throw error;
    }
  },

  async get(name: string): Promise<AdapterConfig> {
    const response = await apiRequest(`/api/adapters/${name}`);
    const data = await response.json();
    return AdapterConfigSchema.parse(data);
  },

  async create(data: { name: string; config: AdapterConfig }): Promise<void> {
    await apiRequest('/api/adapters', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  },

  async update(name: string, config: AdapterConfig): Promise<void> {
    await apiRequest(`/api/adapters/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
  },

  async delete(name: string): Promise<void> {
    await apiRequest(`/api/adapters/${name}`, {
      method: 'DELETE',
    });
  },

  async getSchema(request: {
    connection: string;
    source: AdapterSource;
  }): Promise<ColumnInfo[]> {
    const response = await apiRequest('/api/adapters/get-schema', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    });
    const data = await response.json();
    return z.array(ColumnInfoSchema).parse(data);
  },

  async testSchema(request: {
    connection: string;
    source: AdapterSource;
    columns: Array<{ name: string; type: string; description?: string }>;
  }): Promise<void> {
    await apiRequest('/api/adapters/test-schema', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    });
  },
};

const models = {
  async list(): Promise<ModelSummary[]> {
    const response = await apiRequest('/api/models');
    const data = await response.json();
    return z.array(ModelSummarySchema).parse(data);
  },

  async get(name: string): Promise<ModelConfig> {
    const response = await apiRequest(`/api/models/${name}`);
    const data = await response.json();
    return ModelConfigSchema.parse(data);
  },

  async create(data: { name: string; config: ModelConfig }): Promise<void> {
    await apiRequest('/api/models', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  },

  async update(name: string, config: ModelConfig): Promise<void> {
    await apiRequest(`/api/models/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
  },

  async delete(name: string): Promise<void> {
    await apiRequest(`/api/models/${name}`, {
      method: 'DELETE',
    });
  },
};

const connections = {
  async list(): Promise<ConnectionSummary[]> {
    try {
      const response = await apiRequest('/api/connections');
      const data = await response.json();
      return z.array(ConnectionSummarySchema).parse(data);
    } catch (error) {
      if (error instanceof SyntaxError && error.message.includes('JSON')) {
        console.error('JSON parse error in connections.list:', error);
        throw new ApiError(
          'Invalid JSON response from server. Check server logs for details.',
        );
      }
      throw error;
    }
  },

  async get(name: string): Promise<ConnectionConfig> {
    const response = await apiRequest(`/api/connections/${name}`);
    const data = await response.json();
    return ConnectionConfigSchema.parse(data);
  },

  async create(data: {
    name: string;
    config: ConnectionConfig;
  }): Promise<void> {
    await apiRequest('/api/connections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  },

  async update(name: string, config: ConnectionConfig): Promise<void> {
    await apiRequest(`/api/connections/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
  },

  async delete(name: string): Promise<void> {
    await apiRequest(`/api/connections/${name}`, {
      method: 'DELETE',
    });
  },

  async test(config: TestConnectionConfig): Promise<void> {
    await apiRequest('/api/connections/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
  },
};

const QueryListResponseSchema = z.object({
  queries: z.record(z.string(), QueryConfigSchema),
});

export interface QueryResult {
  data: Record<string, string[]>;
  row_count: number;
  column_count: number;
}

const queries = {
  async list(): Promise<QuerySummary[]> {
    const response = await apiRequest('/api/queries');
    const data = await response.json();
    const listResponse = QueryListResponseSchema.parse(data);
    return Object.entries(listResponse.queries).map(([name, config]) => ({
      name,
      description: config.description,
    }));
  },

  async get(name: string): Promise<QueryConfig> {
    const response = await apiRequest(`/api/queries/${name}`);
    const data = await response.json();
    return QueryConfigSchema.parse(data);
  },

  async save(data: {
    name: string;
    description?: string;
    sql: string;
  }): Promise<void> {
    await apiRequest('/api/queries', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: data.name,
        config: {
          description: data.description,
          sql: data.sql,
        },
      }),
    });
  },

  async update(
    name: string,
    data: {
      name: string;
      description?: string;
      sql: string;
    },
  ): Promise<void> {
    await apiRequest(`/api/queries/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        description: data.description,
        sql: data.sql,
      }),
    });
  },

  async delete(name: string): Promise<void> {
    await apiRequest(`/api/queries/${name}`, {
      method: 'DELETE',
    });
  },

  async execute(sql: string): Promise<QueryResult> {
    const response = await apiRequest('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }),
    });
    return response.json();
  },
};

const dashboards = {
  async list(): Promise<DashboardSummary[]> {
    const response = await apiRequest('/api/dashboards');
    const data = await response.json();
    return z.array(DashboardSummarySchema).parse(data);
  },

  async get(name: string): Promise<DashboardConfig> {
    const response = await apiRequest(`/api/dashboards/${name}`);
    const data = await response.json();
    return DashboardConfigSchema.parse(data);
  },

  async getData(name: string): Promise<{ labels: object[]; values: object[] }> {
    const response = await apiRequest(`/api/dashboards/${name}/data`);
    return response.json();
  },

  async create(data: { name: string; config: DashboardConfig }): Promise<void> {
    await apiRequest('/api/dashboards', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  },

  async update(name: string, data: Partial<DashboardConfig>): Promise<void> {
    await apiRequest(`/api/dashboards/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
  },

  async delete(name: string): Promise<void> {
    await apiRequest(`/api/dashboards/${name}`, {
      method: 'DELETE',
    });
  },
};

const pipeline = {
  async listPipelines(): Promise<Pipeline[]> {
    const response = await apiRequest('/api/pipelines');
    const data = await response.json();
    return data;
  },

  async getStatus(): Promise<Pipeline> {
    const response = await apiRequest('/api/pipeline');
    const data = await response.json();
    return PipelineSchema.parse(data);
  },

  async getGraph(): Promise<GraphData> {
    const response = await apiRequest('/api/graph');
    const data = await response.json();
    return GraphDataSchema.parse(data);
  },

  async run(): Promise<void> {
    await apiRequest('/api/pipeline/run', {
      method: 'POST',
    });
  },

  async runNode(nodeName: string): Promise<void> {
    await apiRequest('/api/pipeline/run-node', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_name: nodeName }),
    });
  },

  async stop(): Promise<void> {
    await apiRequest('/api/pipeline/stop', {
      method: 'POST',
    });
  },
};

export const api = {
  adapters,
  models,
  connections,
  queries,
  dashboards,
  pipeline,
} as const;

export default api;

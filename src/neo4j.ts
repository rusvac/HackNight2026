// Neo4j HTTP Query API client
// Supports both authenticated and no-auth modes

const NEO4J_QUERY_URL = process.env.NEO4J_QUERY_URL || "https://ff75d054.databases.neo4j.io/db/neo4j/query/v2";
const NEO4J_USER = process.env.NEO4J_USER || "";
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD || "";

// Build headers - only add auth if credentials are provided
function getHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  // Only add auth header if both user and password are set
  if (NEO4J_USER && NEO4J_PASSWORD) {
    const credentials = Buffer.from(`${NEO4J_USER}:${NEO4J_PASSWORD}`).toString("base64");
    headers["Authorization"] = `Basic ${credentials}`;
  }

  return headers;
}

// Execute a Cypher query via HTTP API
export async function runQuery(cypher: string, parameters: Record<string, any> = {}): Promise<any[]> {
  const response = await fetch(NEO4J_QUERY_URL, {
    method: "POST",
    headers: getHeaders(),
    body: JSON.stringify({
      statement: cypher,
      parameters,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Neo4j query failed: ${response.status} - ${text}`);
  }

  const result = await response.json();
  
  // Handle errors in response
  if (result.errors && result.errors.length > 0) {
    throw new Error(`Neo4j error: ${result.errors[0].message}`);
  }

  // Transform result to array of records
  const data = result.data || {};
  const fields = data.fields || [];
  const values = data.values || [];

  return values.map((row: any[]) => {
    const record: Record<string, any> = {};
    fields.forEach((field: string, i: number) => {
      record[field] = row[i];
    });
    return record;
  });
}

// Initialize schema/indexes on startup
export async function initializeNeo4j(): Promise<void> {
  try {
    await runQuery(`
      CREATE INDEX identifier_value IF NOT EXISTS
      FOR (i:Identifier) ON (i.value)
    `);
    console.log("✅ Neo4j indexes initialized");
  } catch (error: any) {
    console.log("⚠️ Could not create index:", error?.message || error);
  }
}

// Session-like interface for compatibility with resolvers
export function getSession() {
  return {
    run: async (cypher: string, params: Record<string, any> = {}) => {
      const records = await runQuery(cypher, params);
      return {
        records: records.map((r) => ({
          get: (key: string) => r[key],
          toObject: () => r,
        })),
      };
    },
    close: async () => {
      // No-op for HTTP API
    },
  };
}

export async function closeDriver(): Promise<void> {
  // No-op for HTTP API
}

const authMode = (NEO4J_USER && NEO4J_PASSWORD) ? "Basic Auth" : "No Auth";
console.log(`🔗 Neo4j HTTP API: ${NEO4J_QUERY_URL} (${authMode})`);

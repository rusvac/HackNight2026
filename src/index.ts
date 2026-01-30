import { Elysia } from "elysia";
import { cors } from "@elysiajs/cors";
import { yoga } from "@elysiajs/graphql-yoga";
import { initializeNeo4j, getSession, closeDriver } from "./neo4j";
import { resolvers } from "./graphql/resolvers";

// Read schema file
const typeDefs = await Bun.file("src/graphql/schema.graphql").text();

// Initialize Neo4j on startup
await initializeNeo4j();

const PORT = process.env.PORT || 3000;

const app = new Elysia()
  .use(cors())

  // GraphQL API
  .use(
    yoga({
      typeDefs,
      resolvers,
    })
  )

  // Health check
  .get("/health", () => ({
    status: "ok",
    timestamp: new Date().toISOString(),
    service: "hacknight-redg-neo4j",
    version: "1.0.0",
  }))

  // REST: Get all subjects
  .get("/api/subjects", async () => {
    const session = getSession();
    try {
      const result = await session.run(`
        MATCH (i:Identifier)-[:STATEMENT]->()
        RETURN DISTINCT i.value as subject
        ORDER BY subject
      `);
      const subjects = result.records.map((r: any) => r.get("subject"));
      return { subjects, count: subjects.length };
    } finally {
      await session.close();
    }
  })

  // REST: Get all data (for visualization)
  .get("/api/data", async () => {
    console.log("📊 Fetching all data for visualization");
    const session = getSession();
    try {
      const stmtResult = await session.run(`
        MATCH (i:Identifier)-[s:STATEMENT]->(t)
        RETURN 
          s.id as id,
          i.value as subject_identifier,
          s.predicate as predicate,
          s.object_value as object_value,
          s.source_id as source_id,
          s.timestamp as statement_timestamp,
          s.created_at as created_at
      `);

      const statements = stmtResult.records.map((r: any) => ({
        id: r.get("id"),
        subject_identifier: r.get("subject_identifier"),
        predicate: r.get("predicate"),
        object_value: r.get("object_value"),
        source_id: r.get("source_id"),
        statement_timestamp: r.get("statement_timestamp"),
        created_at: r.get("created_at"),
      }));

      const subjResult = await session.run(`
        MATCH (i:Identifier)-[:STATEMENT]->()
        RETURN DISTINCT i.value as subject
      `);
      const subjects = subjResult.records.map((r: any) => r.get("subject"));

      console.log(`   Returning: ${subjects.length} subjects, ${statements.length} statements`);
      return { statements, subjects };
    } finally {
      await session.close();
    }
  })

  // REST: Search
  .get("/api/search", async ({ query }) => {
    if (!query.q) throw new Error("Query parameter 'q' is required");
    const session = getSession();
    try {
      const result = await session.run(
        `
        MATCH (i:Identifier)-[s:STATEMENT]->(t)
        WHERE toLower(s.object_value) CONTAINS toLower($q)
           OR toLower(i.value) CONTAINS toLower($q)
        RETURN 
          s.id as id,
          i.value as subject_identifier,
          s.predicate as predicate,
          s.object_value as object_value,
          s.source_id as source_id,
          s.timestamp as statement_timestamp,
          s.created_at as created_at
        LIMIT 100
      `,
        { q: query.q }
      );
      const statements = result.records.map((r: any) => ({
        id: r.get("id"),
        subject_identifier: r.get("subject_identifier"),
        predicate: r.get("predicate"),
        object_value: r.get("object_value"),
        source_id: r.get("source_id"),
        statement_timestamp: r.get("statement_timestamp"),
        created_at: r.get("created_at"),
      }));
      return { statements, count: statements.length };
    } finally {
      await session.close();
    }
  })

  // Cleanup on shutdown
  .onStop(async () => {
    await closeDriver();
    console.log("🔌 Neo4j connection closed");
  })

  .listen(PORT);

console.log(`
🚀 HACKNIGHT REDG - Neo4j Edition

📍 Server: http://${app.server?.hostname}:${app.server?.port}

🔗 Endpoints:
   GET  /health          ✅ Health check
   POST /graphql         🔮 GraphQL API
   GET  /graphql         🔮 GraphiQL IDE
   GET  /api/subjects    📋 List all subjects
   GET  /api/data        📊 All data (for viz)
   GET  /api/search?q=   🔎 Search statements

🎨 GraphQL Playground: http://localhost:${PORT}/graphql
`);

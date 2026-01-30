import { getSession } from "../neo4j";

// Helper to convert Neo4j Integer to JS number
const toNumber = (val: any) => (val?.toNumber ? val.toNumber() : val);

// Helper to format statement from Neo4j result
const formatStatement = (record: any) => ({
  id: record.get("id"),
  subject_identifier: record.get("subject"),
  predicate: record.get("predicate"),
  object_value: record.get("object_value"),
  source_id: record.get("source_id") || null,
  statement_timestamp: record.get("timestamp"),
  created_at: record.get("created_at"),
});

export const resolvers = {
  Query: {
    // Get all unique subject identifiers
    subjects: async () => {
      const session = getSession();
      try {
        const result = await session.run(`
          MATCH (i:Identifier)-[s:STATEMENT]->()
          RETURN DISTINCT i.value as subject
          ORDER BY subject
        `);
        return result.records.map((r) => r.get("subject"));
      } finally {
        await session.close();
      }
    },

    // Get statements by subject with pagination
    statementsBySubject: async (_: any, { identifier, limit, offset }: any) => {
      const session = getSession();
      try {
        // Get total count
        const countResult = await session.run(
          `
          MATCH (i:Identifier {value: $identifier})-[s:STATEMENT]->()
          RETURN count(s) as total
        `,
          { identifier }
        );
        const totalCount = toNumber(countResult.records[0]?.get("total")) || 0;

        // Get paginated statements
        const result = await session.run(
          `
          MATCH (i:Identifier {value: $identifier})-[s:STATEMENT]->(t)
          RETURN 
            s.id as id,
            i.value as subject,
            s.predicate as predicate,
            s.object_value as object_value,
            s.source_id as source_id,
            s.timestamp as timestamp,
            s.created_at as created_at
          ORDER BY s.created_at DESC
          SKIP $offset LIMIT $limit
        `,
          { identifier, offset, limit }
        );

        const statements = result.records.map(formatStatement);
        return {
          statements,
          totalCount,
          hasMore: offset + statements.length < totalCount,
        };
      } finally {
        await session.close();
      }
    },

    // Get statements by predicate with pagination
    statementsByPredicate: async (_: any, { predicate, limit, offset }: any) => {
      const session = getSession();
      try {
        const countResult = await session.run(
          `
          MATCH ()-[s:STATEMENT {predicate: $predicate}]->()
          RETURN count(s) as total
        `,
          { predicate }
        );
        const totalCount = toNumber(countResult.records[0]?.get("total")) || 0;

        const result = await session.run(
          `
          MATCH (i:Identifier)-[s:STATEMENT {predicate: $predicate}]->(t)
          RETURN 
            s.id as id,
            i.value as subject,
            s.predicate as predicate,
            s.object_value as object_value,
            s.source_id as source_id,
            s.timestamp as timestamp,
            s.created_at as created_at
          ORDER BY s.created_at DESC
          SKIP $offset LIMIT $limit
        `,
          { predicate, offset, limit }
        );

        const statements = result.records.map(formatStatement);
        return {
          statements,
          totalCount,
          hasMore: offset + statements.length < totalCount,
        };
      } finally {
        await session.close();
      }
    },

    // Search statements by object value (contains search)
    searchStatements: async (_: any, { query }: any) => {
      const session = getSession();
      try {
        const result = await session.run(
          `
          MATCH (i:Identifier)-[s:STATEMENT]->(t)
          WHERE toLower(s.object_value) CONTAINS toLower($query)
             OR toLower(i.value) CONTAINS toLower($query)
          RETURN 
            s.id as id,
            i.value as subject,
            s.predicate as predicate,
            s.object_value as object_value,
            s.source_id as source_id,
            s.timestamp as timestamp,
            s.created_at as created_at
          LIMIT 100
        `,
          { query }
        );
        return result.records.map(formatStatement);
      } finally {
        await session.close();
      }
    },

    // Get single statement by ID
    statement: async (_: any, { id }: any) => {
      const session = getSession();
      try {
        const result = await session.run(
          `
          MATCH (i:Identifier)-[s:STATEMENT {id: $id}]->(t)
          RETURN 
            s.id as id,
            i.value as subject,
            s.predicate as predicate,
            s.object_value as object_value,
            s.source_id as source_id,
            s.timestamp as timestamp,
            s.created_at as created_at
        `,
          { id }
        );
        return result.records[0] ? formatStatement(result.records[0]) : null;
      } finally {
        await session.close();
      }
    },

    // Get all statements with pagination
    paginatedStatements: async (_: any, { limit, offset }: any) => {
      const session = getSession();
      try {
        const countResult = await session.run(`
          MATCH ()-[s:STATEMENT]->()
          RETURN count(s) as total
        `);
        const totalCount = toNumber(countResult.records[0]?.get("total")) || 0;

        const result = await session.run(
          `
          MATCH (i:Identifier)-[s:STATEMENT]->(t)
          RETURN 
            s.id as id,
            i.value as subject,
            s.predicate as predicate,
            s.object_value as object_value,
            s.source_id as source_id,
            s.timestamp as timestamp,
            s.created_at as created_at
          ORDER BY s.created_at DESC
          SKIP $offset LIMIT $limit
        `,
          { offset, limit }
        );

        const statements = result.records.map(formatStatement);
        return {
          statements,
          totalCount,
          hasMore: offset + statements.length < totalCount,
        };
      } finally {
        await session.close();
      }
    },
  },

  Mutation: {
    // Create a new statement
    createStatement: async (_: any, { input }: any) => {
      const session = getSession();
      const id = crypto.randomUUID();
      const now = new Date().toISOString();
      const timestamp = input.statement_timestamp || now;

      try {
        await session.run(
          `
          MERGE (subj:Identifier {value: $subject})
          MERGE (obj:Identifier {value: $object_value})
          CREATE (subj)-[s:STATEMENT {
            id: $id,
            predicate: $predicate,
            object_value: $object_value,
            source_id: $source_id,
            timestamp: $timestamp,
            created_at: $created_at
          }]->(obj)
          RETURN s.id as id
        `,
          {
            id,
            subject: input.subject_identifier,
            predicate: input.predicate,
            object_value: JSON.stringify(input.object_value),
            source_id: input.source_id || null,
            timestamp,
            created_at: now,
          }
        );

        return {
          id,
          subject_identifier: input.subject_identifier,
          predicate: input.predicate,
          object_value: input.object_value,
          source_id: input.source_id || null,
          statement_timestamp: timestamp,
          created_at: now,
        };
      } finally {
        await session.close();
      }
    },

    // Delete a statement
    deleteStatement: async (_: any, { id }: any) => {
      const session = getSession();
      try {
        const result = await session.run(
          `
          MATCH ()-[s:STATEMENT {id: $id}]->()
          DELETE s
          RETURN count(s) as deleted
        `,
          { id }
        );
        const deleted = toNumber(result.records[0]?.get("deleted")) > 0;
        return {
          success: deleted,
          message: deleted ? "Statement deleted" : "Statement not found",
          id,
        };
      } finally {
        await session.close();
      }
    },

    // Simple ingest - just creates statements from raw data
    ingest: async (_: any, { data, format, sourceName }: any) => {
      // For hackathon: simple CSV parsing
      // Format: subject,predicate,object per line
      const session = getSession();
      let statementsCreated = 0;
      const entities = new Set<string>();

      try {
        const lines = data.split("\n").filter((l: string) => l.trim());
        const now = new Date().toISOString();

        for (const line of lines) {
          const [subject, predicate, object_value] = line.split(",").map((s: string) => s.trim());
          if (!subject || !predicate || !object_value) continue;

          const id = crypto.randomUUID();
          await session.run(
            `
            MERGE (subj:Identifier {value: $subject})
            MERGE (obj:Identifier {value: $object_value})
            CREATE (subj)-[:STATEMENT {
              id: $id,
              predicate: $predicate,
              object_value: $object_value,
              source_id: $sourceName,
              timestamp: $now,
              created_at: $now
            }]->(obj)
          `,
            { id, subject, predicate, object_value, sourceName, now }
          );
          statementsCreated++;
          entities.add(subject);
        }

        return {
          success: true,
          message: `Ingested ${statementsCreated} statements`,
          entities_created: entities.size,
          statements_created: statementsCreated,
        };
      } finally {
        await session.close();
      }
    },
  },
};

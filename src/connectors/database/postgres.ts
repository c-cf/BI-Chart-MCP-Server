import { Pool, PoolClient } from 'pg';
import { v4 as uuidv4 } from 'uuid';
import { Connection, DatabaseConfig } from '../index';
import { logger } from '../../utils/logger';

/**
 * PostgreSQL connector implementation
 */
export class PostgresConnector implements Connection {
  id: string;
  type: string = 'postgres';
  private pool: Pool;
  private client: PoolClient | null = null;

  /**
   * Creates a new PostgreSQL connector
   * @param config - Database configuration
   */
  constructor(config: DatabaseConfig) {
    this.id = uuidv4();
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
    });
    
    logger.info(`Created PostgreSQL connector: ${this.id}`);
  }

  /**
   * Executes a SQL query
   * @param query - SQL query string
   * @returns Query results
   */
  async execute(query: string): Promise<any[]> {
    logger.info(`Executing query on PostgreSQL connector ${this.id}`);
    
    if (!this.client) {
      this.client = await this.pool.connect();
    }
    
    try {
      const result = await this.client.query(query);
      return result.rows;
    } catch (error) {
      logger.error(`Query execution error: ${error.message}`);
      throw error;
    }
  }

  /**
   * Closes the database connection
   */
  async close(): Promise<void> {
    if (this.client) {
      this.client.release();
      this.client = null;
    }
    
    await this.pool.end();
    logger.info(`Closed PostgreSQL connector: ${this.id}`);
  }
}

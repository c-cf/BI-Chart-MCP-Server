import dotenv from 'dotenv';
import { startServer } from './mcp/server';
import { logger } from './utils/logger';

// Load environment variables
dotenv.config();

// Get port from environment or use default
const port = process.env.PORT ? parseInt(process.env.PORT, 10) : 9000;

// Start the MCP server
try {
  startServer(port);
  logger.info(`Data BI MCP Server started on port ${port}`);
} catch (error) {
  logger.error('Failed to start server:', error);
  process.exit(1);
}

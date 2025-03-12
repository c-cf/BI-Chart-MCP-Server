import { createServer } from 'http';
import { Server as SSEServer } from 'sse';
import { handleRequest } from './handlers';
import { logger } from '../utils/logger';

/**
 * Creates and starts the MCP server
 * @param port - The port to listen on
 * @returns The server instance
 */
export function startServer(port: number = 9000) {
  const server = createServer((req, res) => {
    if (req.method === 'POST') {
      let body = '';
      
      req.on('data', (chunk) => {
        body += chunk.toString();
      });
      
      req.on('end', async () => {
        try {
          const requestData = JSON.parse(body);
          const response = await handleRequest(requestData);
          
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(response));
        } catch (error) {
          logger.error('Error handling request:', error);
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Internal server error' }));
        }
      });
    } else {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method not allowed' }));
    }
  });
  
  // Setup Server-Sent Events for streaming responses
  const sseServer = new SSEServer(server);
  
  sseServer.on('connection', (client) => {
    logger.info('Client connected to SSE');
    
    client.on('close', () => {
      logger.info('Client disconnected from SSE');
    });
  });
  
  server.listen(port, () => {
    logger.info(`MCP server listening on port ${port}`);
  });
  
  return server;
}

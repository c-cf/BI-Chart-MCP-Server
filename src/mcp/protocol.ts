/**
 * MCP request structure following JSON-RPC 2.0 specification
 */
export interface MCPRequest {
    jsonrpc: '2.0';
    id: string;
    method: string;
    params: any;
  }
  
  /**
   * MCP response structure
   */
  export interface MCPResponse {
    jsonrpc: '2.0';
    id: string;
    result?: any;
    error?: {
      code: number;
      message: string;
      data?: any;
    };
  }
  
  /**
   * MCP method names for data transformation and visualization
   */
  export enum MCPMethod {
    // Connection methods
    CONNECT_DATABASE = 'connectDatabase',
    CONNECT_FILE = 'connectFile',
    
    // Query methods
    EXECUTE_QUERY = 'executeQuery',
    
    // Transform methods
    FILTER_DATA = 'filterData',
    AGGREGATE_DATA = 'aggregateData',
    JOIN_DATA = 'joinData',
    SORT_DATA = 'sortData',
    
    // Visualization methods
    GENERATE_BAR_CHART = 'generateBarChart',
    GENERATE_LINE_CHART = 'generateLineChart',
    GENERATE_PIE_CHART = 'generatePieChart',
    GENERATE_SCATTER_CHART = 'generateScatterChart',
  }
  
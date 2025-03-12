import { logger } from '../utils/logger';

/**
 * Filter condition operator
 */
export enum FilterOperator {
  EQUALS = '=',
  NOT_EQUALS = '!=',
  GREATER_THAN = '>',
  LESS_THAN = '<',
  CONTAINS = 'contains',
  IN = 'in',
}

/**
 * Filter condition
 */
export interface FilterCondition {
  field: string;
  operator: FilterOperator;
  value: any;
}

/**
 * Filter options
 */
export interface FilterOptions {
  conditions: FilterCondition[];
  combineOperator?: 'and' | 'or'; // Default is 'and'
}

/**
 * Filters data based on specified conditions
 * @param data - The data to filter
 * @param options - Filter options
 * @returns Filtered data
 */
export function filterData(data: any[], options: FilterOptions): any[] {
  logger.info(`Filtering data with ${options.conditions.length} conditions`);
  
  const combineOperator = options.combineOperator || 'and';
  
  return data.filter(item => {
    if (combineOperator === 'and') {
      return options.conditions.every(condition => evaluateCondition(item, condition));
    } else {
      return options.conditions.some(condition => evaluateCondition(item, condition));
    }
  });
}

/**
 * Evaluates a filter condition against a data item
 * @param item - The data item
 * @param condition - The filter condition
 * @returns Whether the condition is satisfied
 */
function evaluateCondition(item: any, condition: FilterCondition): boolean {
  const { field, operator, value } = condition;
  const fieldValue = item[field];
  
  switch (operator) {
    case FilterOperator.EQUALS:
      return fieldValue === value;
    case FilterOperator.NOT_EQUALS:
      return fieldValue !== value;
    case FilterOperator.GREATER_THAN:
      return fieldValue > value;
    case FilterOperator.LESS_THAN:
      return fieldValue < value;
    case FilterOperator.CONTAINS:
      return String(fieldValue).includes(String(value));
    case FilterOperator.IN:
      return Array.isArray(value) && value.includes(fieldValue);
    default:
      throw new Error(`Unsupported filter operator: ${operator}`);
  }
}

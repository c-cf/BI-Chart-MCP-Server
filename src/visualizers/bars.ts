import { ChartJSNodeCanvas } from 'chartjs-node-canvas';
import fs from 'fs';
import path from 'path';
import { logger } from '../utils/logger';

/**
 * Bar chart options
 */
export interface BarChartOptions {
  title?: string;
  xAxisLabel?: string;
  yAxisLabel?: string;
  xAxisField: string;
  yAxisField: string;
  groupByField?: string;
  width?: number;
  height?: number;
  colors?: string[];
  horizontal?: boolean;
  outputPath?: string;
}

/**
 * Generates a bar chart from data
 * @param data - The data to visualize
 * @param options - Chart options
 * @returns The path to the generated chart image
 */
export async function generateBarChart(data: any[], options: BarChartOptions): Promise<string> {
  logger.info('Generating bar chart');
  
  const width = options.width || 800;
  const height = options.height || 600;
  
  const chartJSNodeCanvas = new ChartJSNodeCanvas({ width, height });
  
  // Prepare chart data
  const labels = [...new Set(data.map(item => item[options.xAxisField]))];
  const dataPoints = labels.map(label => {
    const matchingItem = data.find(item => item[options.xAxisField] === label);
    return matchingItem ? matchingItem[options.yAxisField] : 0;
  });
  
  // Generate chart configuration
  const configuration = {
    type: options.horizontal ? 'horizontalBar' : 'bar',
    data: {
      labels,
      datasets: [{
        label: options.yAxisLabel || options.yAxisField,
        data: dataPoints,
        backgroundColor: options.colors?.[0] || '#4285F4',
      }]
    },
    options: {
      responsive: true,
      title: {
        display: !!options.title,
        text: options.title,
      },
      scales: {
        x: {
          title: {
            display: !!options.xAxisLabel,
            text: options.xAxisLabel,
          }
        },
        y: {
          title: {
            display: !!options.yAxisLabel,
            text: options.yAxisLabel,
          }
        }
      }
    }
  };
  
  // Generate the chart image
  const image = await chartJSNodeCanvas.renderToBuffer(configuration);
  
  // Save the image to file
  const outputDir = path.dirname(options.outputPath || './charts');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  const outputPath = options.outputPath || `./charts/bar_chart_${Date.now()}.png`;
  fs.writeFileSync(outputPath, image);
  
  return outputPath;
}

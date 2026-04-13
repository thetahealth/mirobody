#!/usr/bin/env node
/**
 * Chart Rendering Script
 * Uses @antv/gpt-vis-ssr to render charts
 * 
 * Usage: node render-chart.js <chart-config-json>
 * 
 * Environment Requirements:
 * - Docker: npm install is already in /app/node_modules
 * - Local: Need to set NODE_PATH environment variable to point to node_modules directory
 */

const fs = require('fs');
const path = require('path');

// Module loading — search all known paths in order
const MODULE_NAME = '@antv/gpt-vis-ssr';
const SEARCH_PATHS = [
  '/app/node_modules/' + MODULE_NAME,
  '/app/mirobody/node_modules/' + MODULE_NAME,
  '/usr/lib/node_modules/' + MODULE_NAME,
  '/usr/local/lib/node_modules/' + MODULE_NAME,
];

let render;
try {
  const found = SEARCH_PATHS.find(p => fs.existsSync(p));
  render = found ? require(found).render : require(MODULE_NAME).render;
} catch (error) {
  console.error('Failed to load @antv/gpt-vis-ssr module');
  console.error('Please ensure:');
  console.error('  1. npm install -g @antv/gpt-vis-ssr (for Docker)');
  console.error('  2. npm install in project root (for local development)');
  console.error('  3. NODE_PATH is set correctly (if needed)');
  console.error(`Error: ${error.message}`);
  process.exit(1);
}

async function renderChart(config) {
  try {
    // Parse configuration
    const chartConfig = typeof config === 'string' ? JSON.parse(config) : config;

    // Render chart
    const vis = await render(chartConfig);
    const buffer = vis.toBuffer();
    vis.destroy();

    // Generate filename
    const timestamp = Date.now();
    const random = Math.random().toString(36).substr(2, 9);
    const filename = `chart_${timestamp}_${random}.png`;

    // Return base64 encoded image data and filename
    const base64 = buffer.toString('base64');

    return {
      success: true,
      filename: filename,
      data: base64,
      size: buffer.length
    };
  } catch (error) {
    return {
      success: false,
      error: error.message || 'Chart rendering failed'
    };
  }
}

// Main function
async function main() {
  try {
    // Get configuration from command line arguments
    const configJson = process.argv[2];

    if (!configJson) {
      console.error(JSON.stringify({
        success: false,
        error: 'No chart configuration provided'
      }));
      process.exit(1);
    }

    const result = await renderChart(configJson);
    console.log(JSON.stringify(result));

    if (!result.success) {
      process.exit(1);
    }
  } catch (error) {
    console.error(JSON.stringify({
      success: false,
      error: error.message || 'Unknown error'
    }));
    process.exit(1);
  }
}

// If this script is run directly
if (require.main === module) {
  main();
}

module.exports = { renderChart };


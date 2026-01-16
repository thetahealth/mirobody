#!/usr/bin/env python3
"""
Chart Schema Loader
Load and attach inputSchema to chart service methods
"""

import json
import logging
from pathlib import Path
from typing import Optional, Dict

# Schema directory
CHART_SCHEMA_DIR = Path(__file__).parent / "chart_schema"

# Map of method names to schema file names
schema_mappings = {
    "generate_pie_chart": "pie",
    "generate_line_chart": "line",
    "generate_column_chart": "column",
    "generate_bar_chart": "bar",
    "generate_area_chart": "area",
    "generate_scatter_chart": "scatter",
    "generate_funnel_chart": "funnel",
    "generate_radar_chart": "radar",
    "generate_histogram_chart": "histogram",
    "generate_boxplot_chart": "boxplot",
    "generate_liquid_chart": "liquid",
    "generate_treemap_chart": "treemap",  # Need to create
    "generate_dual_axes_chart": "dual-axes",
    "generate_venn_chart": "venn",
    "generate_sankey_chart": "sankey",
    # Advanced charts
    "generate_mind_map": "mind-map",
    "generate_organization_chart": "organization-chart",
    "generate_flow_diagram": "flow-diagram",
    "generate_network_graph": "network-graph",
    "generate_fishbone_diagram": "fishbone-diagram",
    # Geographic charts (Chinese only)
    "generate_district_map": "district-map",
    "generate_path_map": "path-map",
    "generate_pin_map": "pin-map",
}


def load_chart_schema(chart_name: str) -> Optional[Dict]:
    """
    Load chart schema from JSON file.

    Args:
        chart_name: Chart name (e.g., 'line', 'pie', 'column')

    Returns:
        inputSchema dictionary or None if not found
    """
    schema_file = CHART_SCHEMA_DIR / f"{chart_name}.json"

    if not schema_file.exists():
        logging.warning(f"Chart schema not found: {schema_file}")
        return None

    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
            return schema_data.get("inputSchema")
    except Exception as e:
        logging.error(f"Failed to load chart schema {chart_name}: {e}")
        return None


def attach_schemas_to_service(chart_service_class):
    """
    Attach inputSchema to all chart service methods.

    Args:
        chart_service_class: ChartService class
    """
    # Load and attach schemas
    for method_name, schema_name in schema_mappings.items():
        if hasattr(chart_service_class, method_name):
            method = getattr(chart_service_class, method_name)
            schema = load_chart_schema(schema_name)
            if schema:
                method.inputSchema = schema
                logging.info(f"Attached schema for {method_name}")
            else:
                logging.warning(f"Schema not found for {method_name} ({schema_name})")
        else:
            logging.debug(f"Method {method_name} not found in ChartService")

    logging.info(f"Schema attachment complete for ChartService")


def attach_meta_to_service(chart_service_class):
    """
    Attach meta information to all chart service methods.

    Args:
        chart_service_class: ChartService class
    """
    for method_name, schema_name in schema_mappings.items():
        if hasattr(chart_service_class, method_name):
            method = getattr(chart_service_class, method_name)
            #     "openai/outputTemplate"     : "ui://widget/upload.html",
            #     "openai/toolInvocation/invoking": "Analyzing your health data",
            #     "openai/toolInvocation/invoked": "Served a health data analysis widget",
            #     "openai/widgetAccessible"   : True,
            #     "openai/resultCanProduceWidget": True,
            method.meta = {
                "openai/outputTemplate"     : "ui://widget/chart_url.html",
                "openai/toolInvocation/invoking": "Drawing chart...",
                "openai/toolInvocation/invoked": "Served a chart widget",
                "openai/widgetAccessible"   : True,
                "openai/resultCanProduceWidget": True,
            }


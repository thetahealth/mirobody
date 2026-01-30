#!/usr/bin/env python3
"""
Chart Service - Mirobody Public Tool
Reference: @antv/mcp-server-chart
"""

import asyncio
import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...utils.utils_files.utils_s3 import aupload_image_with_thumbnail
from ...utils.utils_files.utils_oss import AliOSS
from ...utils.config import safe_read_cfg

# Environment configuration
ENV = os.getenv("ENV", "localdb")
IS_ALIYUN = os.getenv("CLUSTER", "").upper() == "ALIYUN"

# Chart storage directory (unified standard path)
# All environments use: ./.theta/mcp/charts
CHARTS_DIR = Path("./.theta/mcp/charts")


class ChartService:
    """
    Chart Rendering Service

    Reference: @antv/mcp-server-chart
    Supports 25+ chart types with detailed JSON schemas.
    """

    def __init__(self):
        self.name = "Chart Service"
        self.version = "3.0.0"

        # Chart storage directory (unified standard path)
        self.charts_dir = CHARTS_DIR
        self.use_local_fallback = False  # Flag indicating if using local fallback

        # Try to create directory, fall back if permission denied
        try:
            self.charts_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            # Fall back to local directory for development
            logging.warning(f"Cannot create {CHARTS_DIR}, using fallback: {e}")
            self.charts_dir = Path(__file__).parent / "generated_charts"
            self.charts_dir.mkdir(parents=True, exist_ok=True)
            self.use_local_fallback = True  # Mark as using fallback

        # Node.js rendering script path
        self.render_script = Path(__file__).parent / "render-chart.js"

        # Initialize OSS (Aliyun environments only)
        self.oss = AliOSS() if IS_ALIYUN else None

        logging.info(
            f"Chart Service initialized, env={ENV},"
            f"charts_dir={self.charts_dir}, use_local_fallback={self.use_local_fallback}, "
            f"render_script={self.render_script}"
        )

    def _normalize_data(self, data) -> Any:
        """
        Normalize data input (handles string JSON, list, dict).
        Returns parsed data or raises ValueError if invalid.
        """
        if isinstance(data, (list, dict)):
            return data
        
        if isinstance(data, str):
            try:
                parsed_data = json.loads(data)
                if not isinstance(parsed_data, (list, dict)):
                    raise ValueError(f"Invalid JSON type: {type(parsed_data).__name__}")
                return parsed_data
            except json.JSONDecodeError as e:
                raise ValueError(f"JSON parse error: {str(e)}")
        
        raise ValueError(f"Invalid data type: {type(data).__name__}")

    async def _render_advanced_chart(
        self,
        chart_type: str,
        data: list,
        title: str = "",
        width: Optional[int] = 600,
        height: Optional[int] = 400,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Render advanced charts (liquid, sankey, venn, mind-map, organization-chart, flow-diagram, network-graph, fishbone-diagram)

        Based on @antv/mcp-server-chart, each chart type requires specific data format.
        Reference: https://github.com/antvis/mcp-server-chart

        ========================================
        🔧 Common Parameters (All Charts):
        ========================================
        - title: Chart title (default: "")
        - width: Chart width in pixels (default: 600)
        - height: Chart height in pixels (default: 400)
        - theme: "default" (default) | "academy" | "dark"
        - style: dict with common key:
          - texture: "default" (default) | "rough" (hand-drawn style)

        ========================================
        📊 Chart Types and Specific Parameters:
        ========================================

        1. liquid - Liquid fill chart for single value indicator (use this instead of gauge)
           Data: {"percent": 0.75} (0-1 range, where 0.75 = 75%)
           Specific Parameters:
           - shape: "circle" (default) | "rect" | "pin" | "triangle"
           - style: dict with keys:
             - backgroundColor: Background color (e.g., "#fff")
             - color: Liquid color (overrides theme color)
             - texture: "default" | "rough"

        2. sankey - Sankey diagram for flow visualization
           Data: [{"source": "from", "target": "to", "value": number}]
           Specific Parameters:
           - nodeAlign: "left" | "right" | "justify" | "center" (default)
           - style: dict with keys:
             - backgroundColor: Background color
             - palette: Color array (e.g., ["#1890ff", "#52c41a"])
             - texture: "default" | "rough"

        3. venn - Venn diagram for set relationships
           Data: [{"label": "A", "value": 10, "sets": ["A"]}, {"label": "AB", "value": 5, "sets": ["A","B"]}]
           Specific Parameters:
           - style: dict with keys:
             - backgroundColor: Background color
             - palette: Color array
             - texture: "default" | "rough"

        4. mind-map - Mind map for idea organization
           Data: {"name": "root", "children": [{"name": "child1", "children": [...]}]}
           Specific Parameters:
           - style: dict with keys:
             - texture: "default" | "rough"

        5. organization-chart - Organization hierarchy chart
           Data: {"name": "CEO", "description": "title", "children": [...]}
           Specific Parameters:
           - orient: "vertical" (default) | "horizontal" (recommended when depth > 3)
           - style: dict with keys:
             - texture: "default" | "rough"

        6. flow-diagram - Flow diagram for process visualization
           Data: {"nodes": [{"name": "node1"}], "edges": [{"source": "node1", "target": "node2", "name": "edge_label"}]}
           Specific Parameters:
           - style: dict with keys:
             - texture: "default" | "rough"

        7. network-graph - Network graph for relationship visualization
           Data: {"nodes": [{"name": "node"}], "edges": [{"source": "A", "target": "B", "name": "relationship"}]}
           Specific Parameters:
           - style: dict with keys:
             - texture: "default" | "rough"

        8. fishbone-diagram - Fishbone diagram for cause analysis
           Data: {"name": "problem", "children": [{"name": "cause", "children": [...]}]}
           Specific Parameters:
           - style: dict with keys:
             - texture: "default" | "rough"

        Note: gauge, heatmap, treemap, violin are NOT supported

        Args:
            chart_type: Chart type (liquid/sankey/venn/mind-map/organization-chart/flow-diagram/network-graph/fishbone-diagram)
            data: Chart data (format depends on chart_type, see above for each type's data structure)
            title: Chart title (default: "")
            width: Chart width in pixels (default: 600)
            height: Chart height in pixels (default: 400)
            user_info: User information (automatically provided by system)
            **kwargs: Additional chart configuration parameters (see above for each chart type's supported parameters)

        Returns:
            {"success": True/False, "url": "chart URL", "message": "message", "error": "error message"}

        Examples:
            # Liquid chart with custom shape and color
            await render_advanced_chart("liquid", {"percent": 0.75}, "Performance: 75%",
                                       shape="circle", style={"color": "#52c41a"})

            # Sankey diagram with custom node alignment and palette
            await render_advanced_chart("sankey", [{"source": "A", "target": "B", "value": 100}], "Flow",
                                       nodeAlign="left", style={"palette": ["#1890ff", "#52c41a"]})

            # Organization chart with horizontal orientation and dark theme
            await render_advanced_chart("organization-chart", {"name": "CEO", "children": [...]},
                                       orient="horizontal", theme="dark")
        """
        try:
            # Normalize data input (handle string JSON, list, dict)
            data = self._normalize_data(data)
            
            # Build chart configuration
            chart_config = {
                "type": chart_type,
                "data": data,
            }

            if title:
                chart_config["title"] = title
            if width:
                chart_config["width"] = width
            if height:
                chart_config["height"] = height

            # Add other configuration parameters
            chart_config.update(kwargs)

            logging.info(f"Rendering chart: type={chart_type}, data_length={len(data)}")

            # Call Node.js rendering script
            result = await self._call_node_renderer(chart_config)

            if not result.get("success"):
                return {
                    "success": False,
                    "error": result.get("error", "Chart rendering failed")
                }

            # Save image and upload to cloud storage
            filename = result["filename"]
            base64_data = result["data"]

            # Decode base64 data
            image_data = base64.b64decode(base64_data)

            # Upload to different storage based on environment
            image_url = await self._upload_chart(image_data, filename)

            logging.info(f"Chart rendered and uploaded successfully: {image_url}")

            return {
                "success": True,
                "url": image_url,
                "filename": filename,
                "file_key": f"charts/{filename}",
                "thumbnail_key": f"thumb_charts/{filename}",
                "message": f"Chart generated successfully: {title or chart_type}",
                "is_chart": True,  # 🆕 Chart marker
                "chart_title": title or chart_type  # 🆕 Chart title
            }

        except Exception as e:
            logging.error(f"Chart rendering failed: {str(e)}")
            return {
                "success": False,
                "error": f"Chart rendering failed: {str(e)}"
            }

    async def _upload_chart(self, image_data: bytes, filename: str) -> str:
        """Upload chart to cloud storage or save locally"""
        try:
            s3_key = f"charts/{filename}"

            # Step 1: Save locally first (in any environment, as backup)
            filepath = self.charts_dir / filename
            filepath.write_bytes(image_data)
            logging.info(f"Chart saved locally: {filepath}")

            # Step 2: Decide whether to upload to cloud storage based on environment
            if ENV == "localdb" or ENV == "local":
                # Local development environment: save locally only, no cloud upload
                # Use MCP_PUBLIC_URL to build complete URL
                base_url = safe_read_cfg("MCP_PUBLIC_URL", "http://localhost:18080")
                image_url = f"{base_url}/charts/{filename}"
                logging.info(f"Local env, using local URL: {image_url}")

            elif IS_ALIYUN and self.oss:
                # Aliyun: upload to OSS (expires in 6 months = 180 * 24 * 3600 = 15552000 seconds)
                upload_result = self.oss.upload_file(
                    file_data=image_data,
                    file_name=filename,
                    directory="charts",
                    content_type="image/png",
                    expires=15552000
                )

                if not upload_result.get("success"):
                    raise Exception(f"OSS upload failed: {upload_result.get('error')}")

                image_url = upload_result["url"]
                logging.info(f"Chart uploaded to OSS: {s3_key}")

            else:
                # AWS: upload to S3 (expires in 6 months = 180 * 24 * 3600 = 15552000 seconds)
                image_url = await aupload_image_with_thumbnail(
                    image_data=image_data,
                    original_key=s3_key,
                    content_type="image/png",
                    expires_in=15552000
                )

                logging.info(f"Chart uploaded to S3: {s3_key}")

            return image_url

        except Exception as e:
            logging.error(f"Failed to upload chart: {str(e)}")
            # Final fallback: ensure file is saved locally
            try:
                filepath = self.charts_dir / filename
                if not filepath.exists():
                    filepath.write_bytes(image_data)
            except Exception:
                pass
            return f"/charts/{filename}"

    async def _render_chart(
        self,
        chart_type: str,
        data: list,
        title: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Internal method for rendering common charts.

        Args:
            chart_type: Chart type
            data: Chart data array
            title: Chart title
            width: Chart width
            height: Chart height
            user_info: User information
            **kwargs: Additional configuration

        Returns:
            Dictionary containing success, url, message fields
        """
        try:
            # Normalize data input (handle string JSON, list, dict)
            data = self._normalize_data(data)
            
            # Build chart configuration
            chart_config = {
                "type": chart_type,
                "data": data,
            }

            if title:
                chart_config["title"] = title
            if width:
                chart_config["width"] = width
            if height:
                chart_config["height"] = height

            # Add other configuration parameters
            chart_config.update(kwargs)

            logging.info(f"Rendering chart: type={chart_type}, data_length={len(data)}")

            logging.info(f"Chart configuration: {chart_config}")

            # Call Node.js renderer
            result = await self._call_node_renderer(chart_config)

            if not result.get("success"):
                return {
                    "success": False,
                    "error": result.get("error", "Chart rendering failed")
                }

            # Save and upload
            filename = result["filename"]
            base64_data = result["data"]
            image_data = base64.b64decode(base64_data)
            image_url = await self._upload_chart(image_data, filename)

            logging.info(f"Chart rendered successfully: {image_url}")

            return {
                "success": True,
                "url": image_url,
                "filename": filename,
                "file_key": f"charts/{filename}",
                "thumbnail_key": f"thumb_charts/{filename}",
                "message": f"Chart generated successfully and will be displayed at the beginning of the response automatically: {title or chart_type}",
                "is_chart": True,  # 🆕 Chart marker
                "chart_title": title or chart_type  # 🆕 Chart title
            }

        except Exception as e:
            logging.error(f"Chart rendering failed: {str(e)}")
            return {
                "success": False,
                "error": f"Chart rendering failed: {str(e)}"
            }

    async def _call_node_renderer(self, chart_config: Dict[str, Any]) -> Dict[str, Any]:
        """Call Node.js rendering script"""
        try:
            config_json = json.dumps(chart_config)

            # Set NODE_PATH environment variable to ensure mirobody's node_modules can be found
            env = os.environ.copy()
            env['NODE_PATH'] = '/app/mirobody/node_modules'

            # Call Node.js script
            process = await asyncio.create_subprocess_exec(
                "node",
                str(self.render_script),
                config_json,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )

            stdout, stderr = await process.communicate()

            # Parse output - Node.js script returns JSON even on failure
            output = stdout.decode()

            # Try to parse stdout first (contains structured error info)
            try:
                result = json.loads(output)
                # If parsing succeeds, return the result (even if success=false)
                return result
            except json.JSONDecodeError:
                # If stdout is not valid JSON, check stderr
                if stderr:
                    error_msg = stderr.decode()
                    logging.error(f"Node.js renderer failed: {error_msg}")
                    return {"success": False, "error": error_msg}
                elif process.returncode != 0:
                    # Process failed but no error info available
                    logging.error(f"Node.js renderer failed with code {process.returncode}")
                    return {"success": False, "error": f"Renderer failed with exit code {process.returncode}"}
                else:
                    # Output is not JSON but process succeeded
                    logging.error(f"Invalid renderer output: {output[:200]}")
                    return {"success": False, "error": "Invalid renderer output format"}

        except FileNotFoundError:
            return {"success": False, "error": "Node.js not found. Please install Node.js."}
        except Exception as e:
            logging.error(f"Renderer execution failed: {str(e)}")
            return {"success": False, "error": f"Renderer execution failed: {str(e)}"}

    async def generate_pie_chart(
        self,
        data: list,
        title: str = "",
        innerRadius: float = 0,
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a pie chart to show the proportion of parts, such as, market share and budget allocation.

        Args:
            data: Data for pie chart. Array of objects where each object contains:
                  - category (string, required): Category name
                  - value (number, required): Numeric value

                  Example: [{"category": "分类一", "value": 27}]

            title: Set the title of chart (default: "")
            innerRadius: Set the innerRadius of pie chart, the value between 0 and 1. Set the pie chart as a donut chart. Set the value to 0.6 or number in [0, 1] to enable it (default: 0)
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if innerRadius is not None and innerRadius > 0:
            kwargs['innerRadius'] = innerRadius
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme

        return await self._render_chart("pie", data, title or None, width, height, user_info=user_info, **kwargs)

    async def generate_line_chart(
        self,
        data: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a line chart to show trends over time, such as, the ratio of Apple computer sales to Apple's profits changed from 2000 to 2016.

        Args:
            data: Data for line chart. Array of objects where each object contains:
                  - time (string, required): Time point
                  - value (number, required): Numeric value
                  - group (string, optional): Group identifier for multiple lines

                  Example: [{"time": "2015", "value": 23}, {"time": "2016", "value": 32}]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - lineWidth (number): Line width for the lines of chart, such as 4
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("line", data, title or None, width, height, user_info=user_info, **kwargs)


    async def generate_column_chart(
        self,
        data: list,
        title: str = "",
        group: bool = True,
        stack: bool = False,
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a column chart, which are best for comparing categorical data, such as, when values are close, column charts are preferable because our eyes are better at judging height than other visual elements like area or angles.

        Args:
            data: Data for column chart. Array of objects where each object contains:
                  - category (string, required): Category name
                  - value (number, required): Numeric value
                  - group (string, optional): Group identifier when grouping or stacking is needed

                  Example (basic): [{"category": "分类一", "value": 10}, {"category": "分类二", "value": 20}]
                  Example (grouped): [{"category": "北京", "value": 825, "group": "油车"}, {"category": "北京", "value": 1000, "group": "电车"}]

            title: Set the title of chart (default: "")
            group: Whether grouping is enabled (default: True). When enabled, column charts require a 'group' field in the data. When `group` is True, `stack` should be False
            stack: Whether stacking is enabled (default: False). When enabled, column charts require a 'group' field in the data. When `stack` is True, `group` should be False
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if group is not None:
            kwargs['group'] = group
        if stack is not None:
            kwargs['stack'] = stack
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("column", data, title or None, width, height, user_info=user_info, **kwargs)

    async def _generate_area_chart(
        self,
        data: list,
        title: str = "",
        stack: bool = False,
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a area chart to show data trends under continuous independent variables and observe the overall data trend, such as, displacement = velocity (average or instantaneous) × time: s = v × t. If the x-axis is time (t) and the y-axis is velocity (v) at each moment, an area chart allows you to observe the trend of velocity over time and infer the distance traveled by the area's size.

        Args:
            data: Data for area chart. Array of objects where each object contains:
                  - time (string, required): Time point
                  - value (number, required): Numeric value
                  - group (string, optional): Group identifier for stacked area charts

                  Example: [{"time": "2018", "value": 99.9}]

            title: Set the title of chart (default: "")
            stack: Whether stacking is enabled (default: False). When enabled, area charts require a 'group' field in the data
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - lineWidth (number): Line width for the lines of chart, such as 4
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if stack is not None:
            kwargs['stack'] = stack
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("area", data, title or None, width, height, user_info=user_info, **kwargs)

    async def _generate_bar_chart(
        self,
        data: list,
        title: str = "",
        group: bool = False,
        stack: bool = True,
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a horizontal bar chart to show data for numerical comparisons among different categories, such as, comparing categorical data and for horizontal comparisons.

        Args:
            data: Data for bar chart. Array of objects where each object contains:
                  - category (string, required): Category name
                  - value (number, required): Numeric value
                  - group (string, optional): Group identifier when grouping or stacking is needed

                  Example (basic): [{"category": "分类一", "value": 10}, {"category": "分类二", "value": 20}]
                  Example (grouped): [{"category": "北京", "value": 825, "group": "油车"}, {"category": "北京", "value": 1000, "group": "电车"}]

            title: Set the title of chart (default: "")
            group: Whether grouping is enabled (default: False). When enabled, bar charts require a 'group' field in the data. When `group` is True, `stack` should be False
            stack: Whether stacking is enabled (default: True). When enabled, bar charts require a 'group' field in the data. When `stack` is True, `group` should be False
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if group is not None:
            kwargs['group'] = group
        if stack is not None:
            kwargs['stack'] = stack
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("bar", data, title or None, width, height, user_info=user_info, **kwargs)

    async def _generate_scatter_chart(
        self,
        data: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a scatter chart to show the relationship between two variables, helps discover their relationship or trends, such as, the strength of correlation, data distribution patterns.

        Args:
            data: Data for scatter chart. Array of objects where each object contains:
                  - x (number, required): X-axis value
                  - y (number, required): Y-axis value
                  - group (string, optional): Group name for the data point

                  Example: [{"x": 10, "y": 15}]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("scatter", data, title or None, width, height, user_info=user_info, **kwargs)

    async def _generate_funnel_chart(
        self,
        data: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a funnel chart to visualize the progressive reduction of data as it passes through stages, such as, the conversion rates of users from visiting a website to completing a purchase.

        Args:
            data: Data for funnel chart. Array of objects where each object contains:
                  - category (string, required): Stage name
                  - value (number, required): Value at that stage
                  Data should be ordered from top to bottom of funnel

                  Example: [
                    {"category": "浏览网站", "value": 50000},
                    {"category": "放入购物车", "value": 35000},
                    {"category": "生成订单", "value": 25000},
                    {"category": "支付订单", "value": 15000},
                    {"category": "完成交易", "value": 8000}
                  ]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme

        return await self._render_chart("funnel", data, title or None, width, height, user_info=user_info, **kwargs)

    async def generate_dual_axes_chart(
        self,
        categories: list,
        series: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a dual axes chart which is a combination chart that integrates two different chart types, typically combining a bar chart with a line chart to display both the trend and comparison of data, such as, the trend of sales and profit over time.

        Args:
            categories: Categories for dual axes chart, such as ['2015', '2016', '2017']
            series: Series for dual axes chart. Array of objects where each object contains:
                    - type (string, required): 'column' or 'line'
                    - data (array of numbers, required): When type is column, represents quantities like [91.9, 99.1, 101.6, 114.4, 121]. When type is line, represents ratios (recommended < 1) like [0.055, 0.06, 0.062, 0.07, 0.075]
                    - axisYTitle (string, optional): Y-axis title for the series

                    Example: [
                      {"type": "column", "data": [91.9, 99.1, 101.6, 114.4, 121], "axisYTitle": "销售额"},
                      {"type": "line", "data": [0.055, 0.06, 0.062, 0.07, 0.075], "axisYTitle": "利润率"}
                    ]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        # Normalize categories and series (handle string JSON inputs)
        categories = self._normalize_data(categories)
        series = self._normalize_data(series)
        
        # Dual axes chart has different data structure
        chart_config = {
            "type": "dual-axes",
            "categories": categories,
            "series": series,
        }

        if title:
            chart_config["title"] = title
        if width:
            chart_config["width"] = width
        if height:
            chart_config["height"] = height
        if style:
            chart_config["style"] = style
        if theme != "default":
            chart_config["theme"] = theme
        if axisXTitle:
            chart_config["axisXTitle"] = axisXTitle

        # Call Node.js renderer directly (bypass render_chart for special structure)
        result = await self._call_node_renderer(chart_config)

        if not result.get("success"):
            return {
                "success": False,
                "error": result.get("error", "Chart rendering failed")
            }

        # Save and upload
        filename = result["filename"]
        base64_data = result["data"]
        image_data = base64.b64decode(base64_data)
        image_url = await self._upload_chart(image_data, filename)

        logging.info(f"Dual axes chart rendered successfully: {image_url}")

        return {
            "success": True,
            "url": image_url,
            "filename": filename,
            "file_key": f"charts/{filename}",
            "thumbnail_key": f"thumb_charts/{filename}",
            "message": f"Chart generated successfully: {title or 'dualAxes'}",
            "is_chart": True,  # 🆕 Chart marker
            "chart_title": title or "Dual Axes Chart"  # 🆕 Chart title
        }

    async def _generate_histogram_chart(
        self,
        data: list,
        title: str = "",
        binNumber: Optional[int] = None,
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a histogram chart to show the frequency of data points within a certain range. It can observe data distribution, such as, normal and skewed distributions, and identify data concentration areas and extreme points.

        Args:
            data: Data for histogram chart. Array of numbers (NOT objects).
                  Example: [78, 88, 60, 100, 95]

            title: Set the title of chart (default: "")
            binNumber: Number of intervals to define the number of intervals in a histogram. When not specified, a built-in value will be used (optional)
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if binNumber is not None:
            kwargs['binNumber'] = binNumber
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("histogram", data, title or None, width, height, user_info=user_info, **kwargs)


    async def _generate_boxplot_chart(
        self,
        data: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        axisXTitle: str = "",
        axisYTitle: str = "",
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a boxplot chart to show data for statistical summaries among different categories, such as, comparing the distribution of data points across categories.

        Args:
            data: Data for boxplot chart. Array of objects where each object contains:
                  - category (string, required): Category of the data point, such as '分类一'
                  - value (number, required): Value of the data point, such as 10
                  - group (string, optional): Optional group for the data point, used for grouping in the boxplot

                  Example: [{"category": "分类一", "value": 10}]
                  Grouped: [{"category": "分类二", "value": 20, "group": "组别一"}]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            axisXTitle: Set the x-axis title of chart (default: "")
            axisYTitle: Set the y-axis title of chart (default: "")
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme
        if axisXTitle:
            kwargs['axisXTitle'] = axisXTitle
        if axisYTitle:
            kwargs['axisYTitle'] = axisYTitle

        return await self._render_chart("boxplot", data, title or None, width, height, user_info=user_info, **kwargs)

    async def generate_radar_chart(
        self,
        data: list,
        title: str = "",
        style: Optional[Dict] = None,
        theme: str = "default",
        width: int = 600,
        height: int = 400,
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a radar chart to display multidimensional data (four dimensions or more), such as, evaluate Huawei and Apple phones in terms of five dimensions: ease of use, functionality, camera, benchmark scores, and battery life.

        Args:
            data: Data for radar chart. Array of objects where each object contains:
                  - name (string, required): Dimension name
                  - value (number, required): Numeric value
                  - group (string, optional): Group identifier for comparing multiple entities

                  Example: [{"name": "Design", "value": 70}]
                  Grouped: [{"name": "Ease of Use", "value": 70, "group": "Huawei"}, {"name": "Ease of Use", "value": 90, "group": "Apple"}]

            title: Set the title of chart (default: "")
            style: Custom style configuration for the chart (optional). Can include:
                   - backgroundColor (string): Background color, such as '#fff'
                   - lineWidth (number): Line width for the lines of chart, such as 4
                   - palette (array of strings): Color palette for the chart
                   - texture (string): 'default' or 'rough' (hand-drawn style)
            theme: Set the theme for the chart (default: 'default'). Options: 'default', 'academy', 'dark'
            width: Set the width of chart in pixels (default: 600)
            height: Set the height of chart in pixels (default: 400)
            user_info: User information (auto-provided by system)

        Returns:
            Dictionary containing success, url, message fields
        """
        kwargs = {}
        if style:
            kwargs['style'] = style
        if theme != "default":
            kwargs['theme'] = theme

        return await self._render_chart("radar", data, title or None, width, height, user_info=user_info, **kwargs)


# 🔧 Load and attach inputSchemas from JSON files
# This must be done after class definition
try:
    from ._chart_service_schema_loader import attach_schemas_to_service, attach_meta_to_service

    attach_schemas_to_service(ChartService)
    attach_meta_to_service(ChartService)
except Exception as e:
    logging.warning(f"Failed to attach chart schemas: {e}")

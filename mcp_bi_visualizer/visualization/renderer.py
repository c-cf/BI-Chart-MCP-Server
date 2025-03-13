import asyncio
import subprocess
import json
import os
import tempfile
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class VisualizationRenderer:
    async def render(self, vega_spec: Dict[str, Any], format: str = "png") -> bytes:
        """
        Render a Vega-Lite specification to an image using a command-line tool.
        
        Args:
            vega_spec: Vega-Lite specification as a dictionary
            format: Image format to produce, e.g., 'png'
            
        Returns:
            Binary image data as bytes
        """
        try:
            # Write spec to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as spec_file:
                json.dump(vega_spec, spec_file)
                spec_file.flush()
                spec_path = spec_file.name
            
            # Prepare output image path
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as out_file:
                out_path = out_file.name
            
            # Use vega-lite CLI (ensure vega-lite or vega-cli is installed)
            cmd = [
                "npx", 
                "vl2png",  # provided by vega-lite-cli
                spec_path,
                "--output", 
                out_path
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Renderer error: {stderr.decode('utf-8', errors='ignore')}")
                raise RuntimeError("Rendering process failed.")
            
            # Read output file data
            with open(out_path, "rb") as img_file:
                image_data = img_file.read()
            
            logger.info("Vega-Lite rendering completed successfully.")
            return image_data
        
        finally:
            # Clean up temp files
            if os.path.exists(spec_path):
                os.remove(spec_path)
            if os.path.exists(out_path):
                os.remove(out_path)
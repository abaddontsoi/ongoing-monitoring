import json
import os
from typing import Dict, Any
import aiofiles

async def load_ongoing_template() -> Dict[str, Any]:
    """
    Load and return the ongoing_template.json file as a dictionary
    """
    template_path = "utilities/ongoing_template.json"
    async with aiofiles.open(template_path, 'r') as f:
        template = json.loads(await f.read())
    return template

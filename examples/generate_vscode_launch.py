#!/bin/env python3

import json
import os
from typing import Any

# Configuration parameters
TARGET_FOLDER = "compass_sdk_examples"
OUTPUT_FILE = ".vscode/launch.json"

# Ensure the .vscode folder exists
os.makedirs(".vscode", exist_ok=True)

# Find all Python modules in the target folder
modules = [
    f
    for f in os.listdir(TARGET_FOLDER)
    if f.endswith(".py") and f != "__init__.py" and f != "utils.py"
]

# Generate launch configurations
launch_config: dict[str, Any] = {"version": "0.2.0", "configurations": []}

for mod in modules:
    module_name = mod[:-3]  # Remove .py extension
    launch_config["configurations"].append(
        {
            "name": f"Debug {module_name}",
            "type": "debugpy",
            "request": "launch",
            "program": f"${{workspaceFolder}}/{TARGET_FOLDER}/{mod}",
            "console": "integratedTerminal",
        }
    )

# Write to launch.json
with open(OUTPUT_FILE, "w") as f:
    print(
        """
// This file was generated automatically by generate_vscode_launch.py script. You can
// run the script again to update this file with new launch configurations.
""".strip(),
        file=f,
    )
    json.dump(launch_config, f, indent=4)

print(f"Updated {OUTPUT_FILE} with {len(modules)} configurations.")

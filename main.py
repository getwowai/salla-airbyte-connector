#!/usr/bin/env python3
"""
Salla Connector - Entry Point

Usage:
    python main.py spec
    python main.py check --config secrets/config.json
    python main.py discover --config secrets/config.json
    python main.py read --config secrets/config.json --catalog catalog.json [--state state.json]
"""

import sys
from airbyte_cdk.entrypoint import launch

from source import SallaSource


def main():
    """Main entry point for the connector."""
    source = SallaSource()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    main()

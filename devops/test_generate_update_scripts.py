#!/usr/bin/env python3
"""Compatibility entry point for the 1.4 update-generator specification.

The 1.3 suite is retired. New work belongs in devops/test_generate_update_scripts_1_4.py.
"""
import os
import runpy

runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_generate_update_scripts_1_4.py"), run_name="__main__")

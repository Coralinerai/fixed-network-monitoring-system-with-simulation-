"""
Run this once before starting the pipeline to create all tables.
    python init_db.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from load.pg_loader import create_tables

if __name__ == "__main__":
    create_tables()
    print("Database initialized successfully.")

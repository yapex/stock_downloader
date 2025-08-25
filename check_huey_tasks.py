#!/usr/bin/env python3
from neo.configs.huey_config import huey
import sqlite3

conn = sqlite3.connect('data/tasks.db')
cursor = conn.cursor()

# Check table schema first
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table'")
print('Table schemas:')
for row in cursor.fetchall():
    print(f'  {row[0]}')

# Check table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('\nTable names:')
for row in cursor.fetchall():
    print(f'  {row[0]}')

# Try to get some data from the first table
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
table_name = cursor.fetchone()[0]
print(f'\nSample data from {table_name}:')
cursor.execute(f'SELECT * FROM {table_name} LIMIT 3')
for row in cursor.fetchall():
    print(f'  {row}')

conn.close()
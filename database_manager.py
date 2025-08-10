#!/usr/bin/env python3
"""
Database Management Tool for Semantic Chat Application
Provides functionality to view, export, reset, and manage the SQLite database
"""

import sqlite3
import json
import csv
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path='users.db'):
        self.db_path = db_path
    
    def get_connection(self):
        """Create and return a database connection."""
        return sqlite3.connect(self.db_path)
    
    def list_tables(self):
        """List all tables in the database."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        conn.close()
        return [table[0] for table in tables]
    
    def view_table(self, table_name, limit=10):
        """View contents of a specific table."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Get data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
        rows = cursor.fetchall()
        
        conn.close()
        
        return {
            'columns': columns,
            'rows': rows,
            'count': len(rows)
        }
    
    def export_table_to_csv(self, table_name, output_dir='exports'):
        """Export a table to CSV format."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create exports directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get data
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Write to CSV
        filename = f"{output_dir}/{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)
        
        conn.close()
        return filename
    
    def export_all_tables(self, output_dir='exports'):
        """Export all tables to CSV files."""
        tables = self.list_tables()
        exported_files = []
        
        for table in tables:
            filename = self.export_table_to_csv(table, output_dir)
            exported_files.append(filename)
        
        return exported_files
    
    def reset_database(self, confirm=False):
        """Reset/clear all data from tables."""
        if not confirm:
            print("⚠️  This will delete all data from all tables!")
            response = input("Type 'yes' to confirm: ")
            if response.lower() != 'yes':
                print("Database reset cancelled.")
                return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        tables = self.list_tables()
        for table in tables:
            cursor.execute(f"DELETE FROM {table};")
        
        conn.commit()
        conn.close()
        
        print("✅ Database reset completed.")
        return True
    
    def get_table_info(self, table_name):
        """Get detailed information about a table."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'columns': [{'name': col[1], 'type': col[2], 'nullable': not col[3]} for col in columns],
            'row_count': row_count
        }
    
    def run_custom_query(self, query):
        """Execute a custom SQL query and return results."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            
            # Check if it's a SELECT query
            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                conn.close()
                return {
                    'columns': columns,
                    'rows': rows,
                    'success': True
                }
            else:
                conn.commit()
                conn.close()
                return {
                    'message': f"Query executed successfully. {cursor.rowcount} rows affected.",
                    'success': True
                }
                
        except Exception as e:
            conn.close()
            return {
                'error': str(e),
                'success': False
            }

def main():
    """Interactive database management interface."""
    db = DatabaseManager()
    
    while True:
        print("\n" + "="*50)
        print("📊 DATABASE MANAGEMENT TOOL")
        print("="*50)
        print("1. List all tables")
        print("2. View table contents")
        print("3. Export table to CSV")
        print("4. Export all tables")
        print("5. Reset database")
        print("6. Get table info")
        print("7. Run custom query")
        print("8. Exit")
        print("="*50)
        
        choice = input("Enter your choice (1-8): ").strip()
        
        if choice == '1':
            tables = db.list_tables()
            print(f"\n📋 Tables found: {len(tables)}")
            for table in tables:
                print(f"  - {table}")
                
        elif choice == '2':
            tables = db.list_tables()
            if not tables:
                print("❌ No tables found!")
                continue
                
            print("\n📋 Available tables:")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            
            try:
                table_idx = int(input("Enter table number: ")) - 1
                if 0 <= table_idx < len(tables):
                    table_name = tables[table_idx]
                    limit = int(input("Enter number of rows to display (default 10): ") or "10")
                    
                    data = db.view_table(table_name, limit)
                    print(f"\n📊 Table: {table_name}")
                    print(f"Columns: {', '.join(data['columns'])}")
                    print(f"Showing {data['count']} rows:")
                    
                    for i, row in enumerate(data['rows'], 1):
                        print(f"\nRow {i}:")
                        for col, val in zip(data['columns'], row):
                            print(f"  {col}: {val}")
                else:
                    print("❌ Invalid table number!")
            except ValueError:
                print("❌ Please enter valid numbers!")
                
        elif choice == '3':
            tables = db.list_tables()
            if not tables:
                print("❌ No tables found!")
                continue
                
            print("\n📋 Available tables:")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            
            try:
                table_idx = int(input("Enter table number: ")) - 1
                if 0 <= table_idx < len(tables):
                    table_name = tables[table_idx]
                    filename = db.export_table_to_csv(table_name)
                    print(f"✅ Exported to: {filename}")
                else:
                    print("❌ Invalid table number!")
            except ValueError:
                print("❌ Please enter valid numbers!")
                
        elif choice == '4':
            files = db.export_all_tables()
            print(f"✅ Exported {len(files)} files:")
            for file in files:
                print(f"  - {file}")
                
        elif choice == '5':
            db.reset_database()
            
        elif choice == '6':
            tables = db.list_tables()
            if not tables:
                print("❌ No tables found!")
                continue
                
            print("\n📋 Available tables:")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            
            try:
                table_idx = int(input("Enter table number: ")) - 1
                if 0 <= table_idx < len(tables):
                    table_name = tables[table_idx]
                    info = db.get_table_info(table_name)
                    print(f"\n📊 Table: {table_name}")
                    print(f"Row count: {info['row_count']}")
                    print("Columns:")
                    for col in info['columns']:
                        nullable = "NULL" if col['nullable'] else "NOT NULL"
                        print(f"  - {col['name']} ({col['type']}) {nullable}")
                else:
                    print("❌ Invalid table number!")
            except ValueError:
                print("❌ Please enter valid numbers!")
                
        elif choice == '7':
            query = input("Enter SQL query: ").strip()
            if query:
                result = db.run_custom_query(query)
                if result['success']:
                    if 'columns' in result:
                        print(f"\n📊 Query Results:")
                        print(f"Columns: {', '.join(result['columns'])}")
                        print(f"Rows: {len(result['rows'])}")
                        
                        for i, row in enumerate(result['rows'][:10], 1):  # Show first 10
                            print(f"Row {i}: {dict(zip(result['columns'], row))}")
                        
                        if len(result['rows']) > 10:
                            print(f"... and {len(result['rows']) - 10} more rows")
                    else:
                        print(result['message'])
                else:
                    print(f"❌ Error: {result['error']}")
            else:
                print("❌ Please enter a valid query!")
                
        elif choice == '8':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice! Please enter 1-8.")

if __name__ == "__main__":
    main()

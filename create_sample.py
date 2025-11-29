import sqlite3
import pandas as pd
import os
import numpy as np

def create_sample():
    """Create rich sample database for testing"""
    os.makedirs("data/sample_databases", exist_ok=True)
    db_path = "data/sample_databases/employees.db"
    
    conn = sqlite3.connect(db_path)
    
    # Rich employee data with hierarchies
    np.random.seed(42)
    data = {
        'emp_id': list(range(1, 31)),
        'name': [f"Employee_{i} {np.random.choice(['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson', 'Taylor'])}" 
                 for i in range(1, 31)],
        'department': np.random.choice(['Engineering', 'Marketing', 'Sales', 'HR', 'Finance'], 30),
        'manager_id': [None, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 
                      8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15],
        'salary': np.random.randint(75000, 160000, 30),
        'hire_year': np.random.randint(2018, 2025, 30),
        'performance': np.round(np.random.uniform(3.0, 5.0, 30), 1)
    }
    
    df = pd.DataFrame(data)
    df.to_sql('employees', conn, index=False)
    conn.close()
    
    print(f"✅ Created sample DB: {db_path}")
    print(f"📊 {len(df)} employees, {df['department'].nunique()} departments")
    print("🎯 Ready for KG conversion!")

if __name__ == "__main__":
    create_sample()
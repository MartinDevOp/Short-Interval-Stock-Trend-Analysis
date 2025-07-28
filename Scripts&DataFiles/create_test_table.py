import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Fetch variables
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")

print("Variables loaded from .env:")
print(f"USERNAME: {USERNAME}, HOST: {HOST}, PORT: {PORT}, DBNAME: {DBNAME}")

# Connect to the database
try:
    connection = psycopg2.connect(
        user=USERNAME,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # SQL to create a test table for stock data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_stocks (
        id SERIAL PRIMARY KEY,
        ticker_symbol VARCHAR(10) NOT NULL,
        company_name VARCHAR(255),
        price DECIMAL(10, 2) NOT NULL,
        volume INTEGER,
        trade_date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Execute the create table query
    cursor.execute(create_table_query)
    print("Test table 'test_stocks' created successfully!")
    
    # Insert some sample data
    insert_sample_data = """
    INSERT INTO test_stocks (ticker_symbol, company_name, price, volume, trade_date) VALUES
    ('AAPL', 'Apple Inc.', 175.50, 50000000, '2025-01-15'),
    ('GOOGL', 'Alphabet Inc.', 2800.25, 1200000, '2025-01-15'),
    ('MSFT', 'Microsoft Corporation', 420.75, 25000000, '2025-01-15'),
    ('TSLA', 'Tesla Inc.', 850.00, 35000000, '2025-01-15'),
    ('AMZN', 'Amazon.com Inc.', 3300.90, 2500000, '2025-01-15')
    ON CONFLICT DO NOTHING;
    """
    
    cursor.execute(insert_sample_data)
    print("Sample data inserted successfully!")
    
    # Query the table to verify data
    cursor.execute("SELECT COUNT(*) FROM test_stocks;")
    count = cursor.fetchone()[0]
    print(f"Total records in test_stocks table: {count}")
    
    # Show some sample data
    cursor.execute("SELECT ticker_symbol, company_name, price FROM test_stocks LIMIT 3;")
    sample_data = cursor.fetchall()
    print("\nSample data from test_stocks table:")
    for row in sample_data:
        print(f"Ticker: {row[0]}, Company: {row[1]}, Price: ${row[2]}")
    
    # Commit the changes
    connection.commit()
    
    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("\nConnection closed successfully.")

except Exception as e:
    print(f"Failed to connect or execute queries: {e}")
    if 'connection' in locals():
        connection.rollback()
        connection.close()

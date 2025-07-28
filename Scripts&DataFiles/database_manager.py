import psycopg2
from dotenv import load_dotenv
import os
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self):
        """Initialize the database manager and load environment variables."""
        load_dotenv()
        
        # Load database configuration from environment
        self.config = {
            'user': os.getenv("USERNAME"),
            'password': os.getenv("PASSWORD"),
            'host': os.getenv("HOST"),
            'port': os.getenv("PORT"),
            'database': os.getenv("DBNAME")
        }
        
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection."""
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(**self.config)
                self.cursor = self.connection.cursor()
                print("Database connection established successfully!")
            return True
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and not self.connection.closed:
                self.connection.close()
                print("Database connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")
    
    def execute_query(self, query, params=None, fetch=True):
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            fetch (bool): Whether to fetch results (True for SELECT, False for INSERT/UPDATE/DELETE)
        
        Returns:
            Query results if fetch=True, otherwise None
        """
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return None
            
            self.cursor.execute(query, params)
            
            if fetch:
                # For SELECT queries
                return self.cursor.fetchall()
            else:
                # For INSERT/UPDATE/DELETE queries
                self.connection.commit()
                return self.cursor.rowcount
                
        except Exception as e:
            print(f"Error executing query: {e}")
            if self.connection:
                self.connection.rollback()
            return None
    
    def execute_single_query(self, query, params=None):
        """
        Execute a query and fetch a single result.
        
        Returns:
            First row of the result, or None if no results
        """
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return None
            
            self.cursor.execute(query, params)
            return self.cursor.fetchone()
                
        except Exception as e:
            print(f"Error executing single query: {e}")
            if self.connection:
                self.connection.rollback()
            return None
    
    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        Automatically commits on success or rolls back on error.
        """
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    raise Exception("Could not establish database connection")
            
            yield self.cursor
            self.connection.commit()
            
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            print(f"Transaction failed: {e}")
            raise
    
    def get_table_info(self, table_name):
        """Get information about a specific table."""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        return self.execute_query(query, (table_name,))
    
    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
        """
        result = self.execute_single_query(query, (table_name,))
        return result[0] if result else False
    
    def get_row_count(self, table_name):
        """Get the number of rows in a table."""
        query = f"SELECT COUNT(*) FROM {table_name};"
        result = self.execute_single_query(query)
        return result[0] if result else 0
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Example usage and testing
if __name__ == "__main__":
    # Method 1: Using the class directly
    db = DatabaseManager()
    
    if db.connect():
        # Test connection with current time
        result = db.execute_single_query("SELECT NOW();")
        print(f"Current time: {result[0]}")
        
        # Check if our test table exists
        if db.table_exists('test_stocks'):
            count = db.get_row_count('test_stocks')
            print(f"test_stocks table exists with {count} rows")
            
            # Get some sample data
            stocks = db.execute_query("SELECT ticker_symbol, price FROM test_stocks LIMIT 3;")
            print("\nSample stock data:")
            for ticker, price in stocks:
                print(f"  {ticker}: ${price}")
        else:
            print("test_stocks table does not exist")
        
        db.disconnect()
    
    print("\n" + "="*50)
    print("Method 2: Using context manager (recommended)")
    
    # Method 2: Using context manager (automatically handles connection)
    with DatabaseManager() as db:
        result = db.execute_single_query("SELECT NOW();")
        print(f"Current time: {result[0]}")
        
        if db.table_exists('test_stocks'):
            count = db.get_row_count('test_stocks')
            print(f"test_stocks table has {count} rows")

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import Optional, Tuple, Any, List


class Conn:
    """
    PostgreSQL database connection handler.
    """

    def __init__(
            self,
            dbname: str,
            user: str,
            password: str,
            host: str = "localhost",
            port: str = "5432",
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None

    def connect(self) -> Tuple[Optional[psycopg2.extensions.connection],
    Optional[psycopg2.extensions.cursor]]:
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                cursor_factory=RealDictCursor  # Return results as dictionaries
            )
            self.cursor = self.conn.cursor()
            print(f"Connected to database '{self.dbname}' successfully.")
        except Exception as e:
            print(f"Error connecting to database '{self.dbname}': {e}")
            self.conn = None
            self.cursor = None
        return self.conn, self.cursor

    def close(self):
        """Close the cursor and the connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")

    # Optional: context manager support
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

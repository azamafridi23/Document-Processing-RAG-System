import os
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv

load_dotenv()

class DatabaseSetup:
    """Manages the database connection and initial table creation."""

    def __init__(self):
        """Initializes the setup by reading the database URL from environment variables."""
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable is not set.")
        self.connection = None

    def connect(self):
        """Establishes and returns a connection to the database."""
        try:
            self.connection = psycopg2.connect(self.db_url)
            return self.connection
        except OperationalError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def create_tables(self):
        """Creates the necessary tables in the database."""
        conn = self.connect()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    id             SERIAL     PRIMARY KEY,
                    file_id        TEXT       NOT NULL UNIQUE,
                    file_name      TEXT       NOT NULL,
                    platform       TEXT       NOT NULL,
                    last_modified  TIMESTAMP  NOT NULL,
                    processed_at   TIMESTAMP
                );
                """)
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS senders (
                      id SERIAL PRIMARY KEY,
                      email TEXT NOT NULL
                  )
              """)
                # NEW: Whitelisted SMS senders
                cur.execute("""
                  CREATE TABLE IF NOT EXISTS sms_senders (
                      id SERIAL PRIMARY KEY,
                      phone TEXT NOT NULL,
                      name TEXT
                  )
              """)
            conn.commit()
            print("✅ file_metadata table is ready.")
        except psycopg2.Error as e:
            print(f"Error creating tables: {e}")
            conn.rollback()
        finally:
            conn.close()

    async def get_allowed_senders(self):
        """Retrieve the allowed senders list from the database"""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch('''
                SELECT email FROM senders;
            ''')
            return [row['email'].strip() for row in rows if row['email'].strip()]

def initialize_database():
    """
    A convenient function to run the database setup process.
    This should be called at application startup.
    """
    db_setup = DatabaseSetup()
    db_setup.create_tables()

if __name__ == "__main__":
    initialize_database()

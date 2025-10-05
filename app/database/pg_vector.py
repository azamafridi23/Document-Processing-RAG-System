import os
import psycopg
import datetime
from uuid import uuid4
from tqdm import tqdm
import pandas as pd
import pymupdf4llm
import asyncio
import logging

from langchain.docstore.document import Document
from langchain_community.document_loaders import DataFrameLoader
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector
from sqlalchemy import text

from dotenv import load_dotenv
import os
load_dotenv()


class PGVectorManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(PGVectorManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.db_name = "postgres"
        # Set up a logger for this class
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def get_connection_string(self, async_mode: bool = False):
        """
        Gets the database connection string from environment variables.
        If async_mode is True, it ensures the string is compatible with an async driver.
        """
        connection_string = os.getenv("DATABASE_URL")
        if async_mode and connection_string.startswith("postgresql://"):
            # Replace the scheme to specify the async driver
            return connection_string.replace("postgresql://", "postgresql+psycopg://", 1)
        return connection_string

    def return_vector_store(self, collection_name, async_mode) -> PGVector:
        connection_string = self.get_connection_string(async_mode=async_mode)
        self.vectorstore = PGVector(
            embeddings=OpenAIEmbeddings(model="text-embedding-3-large"),
            collection_name=collection_name,
            connection=connection_string,
            use_jsonb=True,
            async_mode=async_mode,
        )
        return self.vectorstore

    async def insert_documents(self, collection_name, documents, async_mode=True):
        vectorstore = self.return_vector_store(collection_name, async_mode)
        await vectorstore.aadd_documents(documents)

    def insert_documents_sync(self, collection_name, documents):
        """Synchronously adds documents to the vector store."""
        vectorstore = self.return_vector_store(collection_name, async_mode=False)
        vectorstore.add_documents(documents)

    def get_retriever(self, collection_name, async_mode, k=5):
        vectorstore = self.return_vector_store(collection_name, async_mode)
        retriever = vectorstore.as_retriever(search_kwargs={'k': k})
        return retriever

    def get_allowed_senders(self) -> list[str]:
        """
        Retrieves a list of allowed email senders from the database synchronously.
        This assumes you have a table named 'allowed_senders' with a column 'email_address'.
        """
        conn = psycopg.connect(self.get_connection_string(async_mode=False))
        try:
            with conn.cursor() as cur:
                # IMPORTANT: This assumes a table named 'allowed_senders' and a column 'email_address'.
                cur.execute("SELECT email FROM senders")
                senders = [row[0] for row in cur.fetchall()]
                return senders
        except psycopg.ProgrammingError as e:
            # This specific exception catches "relation does not exist" which is common
            self.logger.error(f"Database query for allowed senders failed, the 'allowed_senders' table might not exist. Error: {e}")
            return []
        except Exception as e:
            self.logger.error(f"An unexpected database error occurred while fetching allowed senders: {e}")
            return []
        finally:
            conn.close()

    def get_all_file_metadata(self):
        """Fetches all file metadata (file_id, file_name) from the database."""
        conn = psycopg.connect(self.get_connection_string(async_mode=False))
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id, file_name FROM file_metadata ORDER BY file_name")
                files = cur.fetchall()
                # Use a dictionary to remove duplicates based on file_id, preserving the first encountered name
                unique_files = {row[0]: row[1] for row in reversed(files)}
                return [{"file_id": file_id, "file_name": file_name} for file_id, file_name in unique_files.items()]
        finally:
            conn.close()

    def get_documents_by_file_ids(self, collection_name: str, file_ids: list[str]) -> list[Document]:
        """Retrieves all documents from the vector store that belong to the given file_ids."""
        if not file_ids:
            return []
        conn = psycopg.connect(self.get_connection_string(async_mode=False))
        try:
            with conn.cursor() as cur:
                # First, get the collection UUID from the collection name
                cur.execute("SELECT uuid FROM langchain_pg_collection WHERE name = %s", (collection_name,))
                collection_uuid_row = cur.fetchone()
                if not collection_uuid_row:
                    self.logger.warning(f"Collection '{collection_name}' not found.")
                    return []
                collection_uuid = collection_uuid_row[0]

                # Query the embedding table using the collection_uuid
                query = """
                    SELECT document, cmetadata FROM langchain_pg_embedding
                    WHERE collection_id = %s AND cmetadata->>'file_id' = ANY(%s)
                """
                cur.execute(query, (collection_uuid, file_ids,))
                docs = []
                for row in cur.fetchall():
                    # The 'cmetadata' column is JSONB, which psycopg2 returns as a dict
                    docs.append(Document(page_content=row[0], metadata=row[1]))
                return docs
        finally:
            conn.close()

    async def close(self):
        self.logger.info(
            "I am in close function *************************************************************************************************************")
        if hasattr(self, "vectorstore") and self.vectorstore is not None:
            if hasattr(self.vectorstore, "_async_engine") and self.vectorstore._async_engine is not None:
                self.logger.info("I am in async engine closing condition")
                self.logger.info(
                    f"type of engine: {type(self.vectorstore._async_engine)}")
                await self.vectorstore._async_engine.dispose(close=True)
            if hasattr(self.vectorstore, "_engine") and self.vectorstore._engine is not None:
                self.logger.info(
                    f"type of engine: {type(self.vectorstore._engine)}")
                self.vectorstore._engine.dispose(close=True)
                self.logger.info("I am in engine closing condition")

    def close_sync(self):
        """Synchronously disposes of the connection engine."""
        self.logger.info("Attempting to close synchronous vector store connection...")
        if hasattr(self, "vectorstore") and self.vectorstore is not None:
            # Ensure we are dealing with a sync engine
            if hasattr(self.vectorstore, "_engine") and self.vectorstore._engine is not None:
                self.logger.info("Disposing of synchronous engine.")
                self.vectorstore._engine.dispose(close=True)

    # NEW: fetch whitelisted SMS numbers
    def get_allowed_phone_numbers(self) -> list[str]:
        """Retrieves a list of allowed E.164 phone numbers from sms_senders table."""
        conn = psycopg.connect(self.get_connection_string(async_mode=False))
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT phone FROM sms_senders")
                numbers = [row[0].strip() for row in cur.fetchall() if row[0] and row[0].strip()]
                return numbers
        except Exception as e:
            self.logger.error(f"An unexpected database error occurred while fetching allowed phone numbers: {e}")
            return []
        finally:
            conn.close()

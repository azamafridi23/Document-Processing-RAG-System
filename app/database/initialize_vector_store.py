import asyncio
from app.data_ingestion.g_drive.pipeline import PGVectorManager
from dotenv import load_dotenv
import os

load_dotenv()

async def create_collection():
    """
    Initializes a new vector store collection if it doesn't already exist.
    """
    collection_name = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")
    print(f"Attempting to initialize collection: '{collection_name}'...")

    # Instantiate the manager using the class from pipeline.py
    vector_manager = PGVectorManager()

    try:
        # Calling the return_vector_store method from your class will automatically 
        # trigger the creation of the necessary database tables.
        vector_manager.return_vector_store(collection_name, async_mode=False)
        
        print(f"✅ Collection '{collection_name}' is ready.")
        print("Associated tables 'langchain_pg_collection' and 'langchain_pg_embedding' are set up.")

    except Exception as e:
        print(f"An error occurred during collection initialization: {e}")
    finally:
        # Gracefully close the connection pool created during initialization
        # The 'close' method is async in your implementation.
        if hasattr(vector_manager, 'close'):
            await vector_manager.close()


if __name__ == "__main__":
    asyncio.run(create_collection()) 
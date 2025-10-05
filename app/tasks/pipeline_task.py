import logging
from app.core.celery_app import celery_app
from app.data_ingestion.google_drive.pipeline import main as run_ingestion_pipeline

# Configure logging
logger = logging.getLogger(__name__)

@celery_app.task(name="app.tasks.run_ingestion_pipeline")
def run_ingestion_pipeline_task():
    """
    Celery task to run the full Google Drive data ingestion and embedding pipeline.
    """
    try:
        logger.info("Starting Google Drive ingestion pipeline via Celery...")
        run_ingestion_pipeline()
        logger.info("Google Drive ingestion pipeline finished successfully.")
        return "Pipeline executed successfully."
    except Exception as e:
        logger.error(f"An error occurred in the ingestion pipeline task: {e}", exc_info=True)
        # Re-raise the exception to mark the task as failed in Celery
        raise 
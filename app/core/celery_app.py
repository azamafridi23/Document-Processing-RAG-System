import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# The REDIS_URL should be added to your .env file
# e.g., REDIS_URL=redis://localhost:6379/0
celery_app = Celery('document_rag_system',
                    broker=os.getenv('REDIS_URL'),
                    backend=os.getenv('REDIS_URL'))

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# The 'imports' setting is crucial for Celery to find your tasks.
celery_app.conf.imports = ('app.tasks.pipeline_task',)
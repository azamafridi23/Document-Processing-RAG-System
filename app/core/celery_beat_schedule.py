from celery.schedules import crontab
from app.core.celery_app import celery_app

# The task import is now handled in celery_app.py, so it's not needed here.

celery_app.conf.beat_schedule = {
    'run-ingestion-pipeline-every-12-hours': {
        'task': 'app.tasks.run_ingestion_pipeline',
        # This runs at the start of every 12th hour (e.g., midnight, noon).
        'schedule': crontab(minute=0, hour='*/12'),
    },
}

# Add robust settings from your reference config
celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1, # Helps manage memory for long-running tasks
    broker_connection_retry_on_startup=True,
) 
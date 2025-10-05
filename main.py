import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from fastapi import Request, Form
from fastapi.responses import Response
from twilio.twiml.messaging_response import MessagingResponse
from app.core.config import settings
from app.api.router import api_router
from app.database.tables import DatabaseSetup
from app.services.gmail_poller import stop_gmail_polling, start_gmail_polling
from app.tasks.pipeline_task import run_ingestion_pipeline_task
from app.services.twilio import create_twiml_response, router as twilio_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # The 'await' keyword has been removed from the line below
    DatabaseSetup().create_tables()

    # Schedule a test Celery task to run in 30 seconds
    # print("Scheduling test Celery task to run in 30 seconds...")
    # run_ingestion_pipeline_task.apply_async(countdown=30)
    # print("Test task scheduled successfully.")

    # Start Gmail polling service if enabled
    if os.getenv("GMAIL_POLLING_ENABLED", "false").lower() == "true":
        poll_interval = int(os.getenv("GMAIL_POLL_INTERVAL", "30"))
        await start_gmail_polling(poll_interval)
        print(f"Gmail polling service started with {poll_interval}s interval")
    else:
        print("Gmail polling service disabled")

    yield

    # Stop Gmail polling service
    await stop_gmail_polling()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    openapi_url="/openapi.json",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/api")
app.include_router(twilio_router)

@app.get("/")
def read_root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

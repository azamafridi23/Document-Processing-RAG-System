# Document Processing RAG System

A production-ready document processing and retrieval system built with FastAPI and LangChain, featuring automated ingestion into PostgreSQL `pgvector`, agentic RAG for Q&A and summarization, and multi-channel interfaces (API, Streamlit, SMS, Email).

## Features

- **Document Processing**: Automated ingestion and processing of documents from various sources
- **Vector Database**: PostgreSQL with pgvector for efficient semantic search
- **Multiple Interfaces**: API, SMS (via Twilio), Email, and Streamlit UI
- **Asynchronous Processing**: Celery for task scheduling and background processing
- **Cloud Integration**: AWS S3 for storage and Google Drive for document retrieval
 - **Auto Data Ingestion to pgvector**: Scheduled pipelines fetch from Google Drive and Gmail, extract text and images, generate embeddings, and upsert them into `pgvector` automatically
 - **AI Agent Capabilities**: Tool-using RAG agent for chat, document Q&A, summarization, and workflow automation across API/Streamlit/SMS/Email

## Project Structure

```
├── app/                      # Main application directory
│   ├── api/                  # API endpoints and routers
│   ├── core/                 # Core configuration and settings
│   ├── data_ingestion/       # Document ingestion pipelines
│   ├── database/             # Database models and connections
│   ├── evaluation/           # System evaluation tools
│   ├── reports/              # Reporting functionality
│   ├── services/             # Business logic services
│   ├── streamlit_app/        # Streamlit UI application
│   └── tasks/                # Celery tasks
├── docker/                   # Docker configuration files
├── scripts/                  # Utility scripts
├── .env.example              # Example environment variables
├── Dockerfile                # Docker build configuration
├── pyproject.toml            # Poetry dependency management
└── supervisord.conf          # Process management configuration
```

## Installation

1. **Clone the repository:**
   ```bash
   git clone <your-repository-url>
   cd <repository-folder>
   ```

2. **Set up environment variables:**
   Copy the example environment file and update with your credentials
   ```bash
   cp example.env .env
   # Edit .env with your configuration
   ```

3. **Install dependencies:**
   ```bash
   poetry install
   ```

## Running the Application

### Development Mode

```bash
# Start the FastAPI server
poetry run uvicorn main:app --reload

# In a separate terminal, start Celery worker
poetry run celery -A app.core.celery_app worker --loglevel=info

# In another terminal, start Celery beat for scheduled tasks
poetry run celery -A app.core.celery_app beat --loglevel=info
```

### Using Docker

```bash
# Build the Docker image
docker build -t document-rag-system .

# Run the container
docker run -d --name document-system -p 8000:8000 -p 5555:5555 document-rag-system
```

## Accessing the Application

- **API Documentation**: http://localhost:8000/docs
- **Streamlit UI**: http://localhost:8501
- **Celery Flower Dashboard**: http://localhost:5555

## Development

### Adding New Documents

The system can ingest documents from:
- Google Drive
- Email attachments
- Direct uploads

### Running Tests

```bash
poetry run pytest
```

## License

[MIT License](LICENSE)
FROM python:3.10-slim

# Install system dependencies including Redis
RUN apt-get update && apt-get install -y \
    supervisor \
    curl \
    awscli \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy poetry files and install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false && poetry install --no-root --all-extras

# Copy application code
COPY . .

# Create log directories
RUN mkdir -p /var/log/supervisor /var/log/docrag

# Create Redis data directory
RUN mkdir -p /var/lib/redis

# Copy supervisor configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy and make AWS script executable
COPY aws.sh /app/aws.sh
RUN chmod +x /app/aws.sh

# Run AWS configuration
RUN /app/aws.sh

# Expose ports
EXPOSE 8000 5555 6379

# Start supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
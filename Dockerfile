FROM python:3.11-slim

# Set working directory
WORKDIR /airbyte/integration_code

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Set environment variables
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

# Run the connector
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

# Labels for Airbyte
LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=source-salla-python

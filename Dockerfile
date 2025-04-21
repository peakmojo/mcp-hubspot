# Use Python base image
FROM python:3.10-slim-bookworm

# Install LevelDB dependencies
RUN apt-get update && apt-get install -y \
    libleveldb-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install the project into `/app`
WORKDIR /app

# Copy the entire project
COPY . /app

# Install the package
RUN pip install --no-cache-dir .

# Create storage and models directories
RUN mkdir -p /app/models /storage

# Pre-download models
RUN python -c "from sentence_transformers import SentenceTransformer; \
    model = SentenceTransformer('all-MiniLM-L6-v2'); \
    model.save('/app/models/all-MiniLM-L6-v2')"

# Set environment variables
ENV HUBSPOT_STORAGE_DIR=/storage

# Run the server
ENTRYPOINT ["mcp-server-hubspot"] 
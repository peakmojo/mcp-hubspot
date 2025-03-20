# Use Python base image
FROM python:3.10-slim-bookworm

# Install the project into `/app`
WORKDIR /app

# Copy the entire project
COPY . /app

# Install the package
RUN pip install --no-cache-dir .

# Run the server
ENTRYPOINT ["mcp-server-hubspot"] 
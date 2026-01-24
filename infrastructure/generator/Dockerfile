FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Expose API port
EXPOSE 8080

# Run generator service
CMD ["python", "-m", "src.generator.main"]

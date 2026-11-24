FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script
COPY feature_counts_consumer.py .

# Make the script executable
RUN chmod +x feature_counts_consumer.py

# Run the consumer script
CMD ["python", "feature_counts_consumer.py"]
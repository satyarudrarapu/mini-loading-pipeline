# Dockerfile — ECS Fargate ingest worker
# Builds a Docker image that runs pipeline/ingest/worker.py.
# This image is pushed to ECR and used by both:
#   - The ingest ECS task (Milestone 2)
#   - The sort ECS task  (Milestone 3)

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir \
    boto3==1.38.28 \
    requests==2.32.3 \
    pydantic==2.10.6

# Copy the entire pipeline package into the container.
COPY pipeline/ ./pipeline/

# Set Python path so `from pipeline import settings` works correctly.
ENV PYTHONPATH=/app

# Do not buffer stdout/stderr — logs appear in CloudWatch immediately.
ENV PYTHONUNBUFFERED=1

# ECS task definition can override this for the sort worker.
CMD ["python", "-m", "pipeline.ingest.worker"]
# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies for scientific packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Testing (used in CI/CD)
FROM builder AS test

WORKDIR /app
COPY . .
RUN python -m pytest tests/

# Stage 3: Production runtime
FROM python:3.11-slim AS production

WORKDIR /app

# Copy only necessary Python packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy application code
COPY Feature_Engineering.ipynb Modeling.ipynb ./
COPY src/ ./src/
COPY config/ ./config/

# Set environment variables for AWS
ENV AWS_DEFAULT_REGION=us-east-1

CMD ["python", "src/main.py"]

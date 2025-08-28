FROM python:3.13-slim

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	git \
	curl \
	&& rm -rf /var/lib/apt/lists/*

# Set up a working directory
WORKDIR /workspace

# Install duckdb cli
RUN curl https://install.duckdb.org | sh

# Upgrade pip and install dlt (and other dev dependencies as needed)
RUN pip install --upgrade pip

# Copy requirements.txt if present and install
COPY requirements.txt ./
RUN pip install -r requirements.txt || true

RUN export PATH='/root/.duckdb/cli/latest':$PATH

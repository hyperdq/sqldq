# Use official Python 3.13 slim image as base
FROM python:3.13-slim

# Set working directory (this will be where your project files are mounted, see devcontainer.json)
WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    coreutils

# Install system dependencies required for PySpark
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    gcc \
    python3-dev \
    make \
    procps \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for Spark
# This script will set the correct JAVA_HOME for any architecture
RUN ARCH=$(dpkg --print-architecture) && \
    JAVA_HOME_PATH="/usr/lib/jvm/java-17-openjdk-${ARCH}" && \
    echo "export JAVA_HOME=${JAVA_HOME_PATH}" >> /etc/bash.bashrc && \
    echo "JAVA_HOME=${JAVA_HOME_PATH}" >> /etc/environment

# Set a default that works for most cases (will be overridden by the script above)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Verify installations and show detected architecture
RUN which psql && psql --version && \
    ARCH=$(dpkg --print-architecture) && \
    echo "Detected architecture: ${ARCH}" && \
    echo "JAVA_HOME will be: /usr/lib/jvm/java-17-openjdk-${ARCH}" && \
    ls -la /usr/lib/jvm/

# Install uv (for manual uv sync workflow)
RUN pip install --no-cache-dir uv

# Set environment variables for Jupyter
ENV JUPYTER_ENABLE_LAB=yes
ENV JUPYTER_TOKEN=""
ENV JUPYTER_ALLOW_ROOT=1

# Expose port for Jupyter
EXPOSE 8888

# Set default command
CMD ["bash"]

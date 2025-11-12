# Dockerfile for PySpark Big Data Analysis Environment
# Base image: Python 3.10 with JDK for Spark
FROM python:3.10-slim-bullseye

# Maintainer information
LABEL maintainer="student@bigdata.edu"
LABEL description="PySpark environment for Chh-OLA Trip Analysis"

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    procps \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Apache Spark 3.5.0 (latest stable)
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Set working directory
WORKDIR /app

# Copy requirements file
COPY docker/requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/output/parquet /app/output/query_results /app/output/logs /app/scripts

# Copy project files
COPY scripts/ /app/scripts/
COPY data/ /app/data/

# Expose Jupyter notebook port and Spark UI port
EXPOSE 8888 4040

# Default command: Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]

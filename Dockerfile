FROM python:3.10-slim

# ===============================
# System dependencies
# ===============================
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ===============================
# Install Java 17 (Temurin)
# ===============================
RUN wget -O- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /usr/share/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# ===============================
# Java env
# ===============================
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV SPARK_MASTER=local[2]
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g
ENV SPARK_SQL_SHUFFLE_PARTITIONS=4
ENV SPARK_SQL_MAX_RECORDS_PER_FILE=250000
ENV DQ_MAX_QUARANTINE_RATE=0.25
ENV MALLOC_ARENA_MAX=2

# ===============================
# App setup
# ===============================
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "pipeline.py"]

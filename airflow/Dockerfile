# ===== Base image: Airflow official =====
ARG AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5-python3.11"
FROM ${AIRFLOW_IMAGE_NAME}

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# ===== System deps: Java 17 + Chromium for Selenium =====
USER root
RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      chromium \
      chromium-driver \
      fonts-liberation \
      libasound2 libatk1.0-0 libcups2 libx11-6 \
      libx11-xcb1 libxcomposite1 libxcursor1 libxi6 \
      libxrandr2 libxdamage1 libgbm1 libgtk-3-0 libnss3 \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV _JAVA_OPTIONS="-Xms256m -Xmx2g"

# Selenium: point to system Chromium & driver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver
ENV SELENIUM_MANAGER=OFF

# ===== Spark + Iceberg JARs (match PySpark 3.4, Scala 2.12) =====
ENV SPARK_JARS_PACKAGES="\
org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262"
ENV PYSPARK_SUBMIT_ARGS="--packages ${SPARK_JARS_PACKAGES} pyspark-shell"

# ===== Python deps =====
# airflow_requirements.txt must be next to this Dockerfile in build context
COPY --chown=airflow:root airflow_requirements.txt /requirements.txt

USER airflow
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu

# Install pip tools, then torch CPU (explicit), then the rest of requirements
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir --prefer-binary torch==2.3.1 \
 && pip install --no-cache-dir --prefer-binary -r /requirements.txt

# ===== HF cache (user: airflow) =====
ENV HF_HOME=/home/airflow/.cache/huggingface
ENV TRANSFORMERS_CACHE=/home/airflow/.cache/huggingface
# Enable after first warm if desired:
# ENV TRANSFORMERS_OFFLINE=1

# ===== Project code =====
ENV PYTHONPATH=/opt/airflow
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
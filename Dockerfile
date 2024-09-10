# Dockerfile
FROM apache/airflow:2.7.1

USER root

# Installer Java et PySpark
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    libpq-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Définir JAVA_HOME pour PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Revenir à l'utilisateur airflow
USER airflow

# Copier le fichier requirements.txt
COPY requirements.txt /opt/airflow/requirements.txt
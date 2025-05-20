FROM python:3.11-slim-bullseye as spark-base

ARG SPARK_VERSION=3.5.5

# Mise à jour et installation des dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        sudo \
        curl \
        vim \
        unzip \
        rsync \
        openjdk-11-jdk \
        build-essential \
        software-properties-common \
        ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# SSH 
RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    mkdir -p /root/.ssh && chmod 700 /root/.ssh

# Variables d'environnement
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop

# Création des répertoires nécessaires
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Téléchargement et extraction de Spark
RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz && \
    tar -xvzf spark.tgz --strip-components=1 && \
    rm spark.tgz

# Installation des dépendances Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Ajout des JARs nécessaires pour accès à MinIO/S3 via s3a
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.11.1026.jar

# Configuration des chemins Spark et Python
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=${SPARK_HOME}/python/:$PYTHONPATH

# Copier la config Spark
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/

# Donner les permissions d'exécution
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Copier le point d'entrée
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Entrée par défaut
ENTRYPOINT ["./entrypoint.sh"]
# Use Flink base image
FROM flink:1.17.1

# Allow passing in custom Python and Flink versions via build args (with defaults).
ARG PYTHON_VERSION=3.8.10
ARG FLINK_VERSION=1.17.1

ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV FLINK_VERSION=${FLINK_VERSION}

# -------------------------------------------------------------------
# 1) Download and install necessary Flink connector JARs.
#    This ensures Kafka connectors (and a faker library) are available.
# -------------------------------------------------------------------
RUN wget -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_VERSION}/flink-connector-kafka-${FLINK_VERSION}.jar \
    && wget -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar \
    && wget -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar \
    && wget -P /opt/flink/lib/ \
      https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar

# -------------------------------------------------------------------
# 2) Install build dependencies for compiling Python from source.
#    In particular, liblzma-dev is required so that the _lzma module
#    is available. We also install other common build headers.
# -------------------------------------------------------------------
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
         build-essential \
         libssl-dev \
         zlib1g-dev \
         libbz2-dev \
         libffi-dev \
         liblzma-dev \
         wget \
         ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# 3) Download, compile, and install the specified Python version.
#    We enable shared libraries so that extension modules (like _lzma)
#    are built correctly. After installation, we symlink 'python3' to 'python'.
# -------------------------------------------------------------------
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
    && tar -xvf Python-${PYTHON_VERSION}.tgz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure --enable-shared --without-ensurepip \
    && make -j$(nproc) \
    && make install \
    && ldconfig /usr/local/lib \
    && cd / \
    && rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz

# Create a symlink so that 'python' and 'pip' point to the new install
RUN ln -sf /usr/local/bin/python3 /usr/local/bin/python \
    && ln -sf /usr/local/bin/pip3 /usr/local/bin/pip

# -------------------------------------------------------------------
# 4) Install PyFlink using pip.
#    We pin the version of apache-flink to match FLINK_VERSION.
# -------------------------------------------------------------------
RUN pip install --no-cache-dir apache-flink==${FLINK_VERSION}

# -------------------------------------------------------------------
# 5) (Optional) Set a working directory for your apps, if desired.
# -------------------------------------------------------------------
WORKDIR /opt/pyflink-apps

# -------------------------------------------------------------------
# 6) By default, Flink’s entrypoint will run. If you need to override,
#    you can switch to a custom entrypoint or CMD here.
# -------------------------------------------------------------------
# (Leave ENTRYPOINT/CMD as-is to delegate to Flink’s standard commands.)


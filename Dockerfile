FROM apache/airflow:2.7.1-python3.11
USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \
    python3 get-pip.py --user

COPY requirements.txt /requirements.txt
RUN pip install --user -r /requirements.txt
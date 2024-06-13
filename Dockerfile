FROM apache/airflow:2.7.1

USER airflow

RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \
    python3 get-pip.py --user

COPY requirements.txt /requirements.txt
RUN pip install --user -r /requirements.txt
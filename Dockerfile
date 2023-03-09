FROM apache/airflow:2.3.4
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


RUN mkdir /opt/droetker
RUN chmod -R 777 /opt/droetker
COPY ./droetker/__init__.py /opt/droetker/
ENV PYTHONPATH "${PYTHONPATH}:/opt"
COPY requirements.txt /opt/droetker

USER airflow
RUN pip install -r /opt/droetker/requirements.txt




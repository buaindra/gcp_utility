FROM apache/beam_python3.9_sdk:2.43.0
# FROM gcr.io/dataflow-geobeam/base 
# FROM gcr.io/dataflow-geobeam/base-py37

WORKDIR /pipeline

# Set the entrypoint to Apache Beam SDK launcher.
COPY --from=apache/beam_python3.9_sdk: 2.43.0 /opt/apache/beam /opt/apache/beam 
ENTRYPOINT [ "/opt/apache/beam/boot" ]

RUN pip install geobeam

# Install the requirements.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
	&& pip install --no-cache-dir -r requirements.txt \
	&& pip check

COPY geobeam_dag.py ./
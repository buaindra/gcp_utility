FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR $(WORKDIR}

#RUN apt-get update && apt-get install -y libffi-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY api_to_bigquery_json_flex.py .

# Do not include appche-beam in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDTR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/api_to_bigquery_json_flex.py"

RUN python3 -m pip install apache-beam[gcp]
RUN pip install -U -r ./requirements.txt
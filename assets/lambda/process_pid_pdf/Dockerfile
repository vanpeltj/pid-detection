FROM public.ecr.aws/lambda/python:3.13
ARG FUNCTION_CODE


# psycopg2 build dependencies
RUN dnf update -y && dnf install gcc postgresql-devel -y

COPY ${FUNCTION_CODE}/requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY ${FUNCTION_CODE}/src/index.py ${LAMBDA_TASK_ROOT}
COPY ${FUNCTION_CODE}/src/helpers.py ${LAMBDA_TASK_ROOT}
COPY ${FUNCTION_CODE}/src/config.yml ${LAMBDA_TASK_ROOT}

COPY commons/data ${LAMBDA_TASK_ROOT}/data
COPY commons/core ${LAMBDA_TASK_ROOT}/core
COPY commons/utils ${LAMBDA_TASK_ROOT}/utils
COPY commons/models ${LAMBDA_TASK_ROOT}/models
COPY commons/schemas ${LAMBDA_TASK_ROOT}/schemas



CMD [ "index.handler" ]
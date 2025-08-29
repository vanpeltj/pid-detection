FROM public.ecr.aws/lambda/python:3.13


# psycopg2 build dependencies
RUN dnf update -y && dnf install gcc postgresql-devel -y
ARG FUNCTION_CODE
#ARG FUNCTION_CODE="lambda/api"

COPY ${FUNCTION_CODE}/requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt
COPY ${FUNCTION_CODE}/src/ ${LAMBDA_TASK_ROOT}

COPY commons/data ${LAMBDA_TASK_ROOT}/data
COPY commons/core ${LAMBDA_TASK_ROOT}/core
COPY commons/utils ${LAMBDA_TASK_ROOT}/utils
COPY commons/models ${LAMBDA_TASK_ROOT}/models
COPY commons/schemas ${LAMBDA_TASK_ROOT}/schemas
CMD [ "app.handler" ]
#CMD ["bash", "-c","uvicorn", "app:app", "--host", "0.0.0.0" , "--port", "8080"]
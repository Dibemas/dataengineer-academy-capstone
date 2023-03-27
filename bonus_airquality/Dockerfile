FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /src

USER 0

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./airquality-ingest.py .

ENTRYPOINT ["python3","airquality-ingest.py"]



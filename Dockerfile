FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /src

USER 0

COPY ./resources/jars/ /opt/spark/jars/
COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./load_weather_data.py .

ENTRYPOINT ["python3","load_weather_data.py"]



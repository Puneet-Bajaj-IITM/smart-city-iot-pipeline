FROM python:3.9

WORKDIR /app

# Build from the files/ directory: docker build -f interoperable_layer/kafka_to_db/Dockerfile -t kaf .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY interoperable_layer/kafka_to_db/kaf_to_db.py ./

ENTRYPOINT ["python", "kaf_to_db.py"]
CMD ["temperature"]

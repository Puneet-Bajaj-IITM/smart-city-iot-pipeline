FROM python:3.9

WORKDIR /app

# Build from the files/ directory: docker build -f interoperable_layer/manager/Dockerfile -t data-receiver .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY interoperable_layer/manager/manager.py interoperable_layer/manager/simulate_node.py ./

ENTRYPOINT ["python", "manager.py"]
CMD ["temperature"]

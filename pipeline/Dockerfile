FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/pipeline
ENV PYTHONPATH=/app
CMD ["python", "pipeline/main.py"]

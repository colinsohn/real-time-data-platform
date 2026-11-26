# syntax=docker/dockerfile:1

FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY platform_core ./platform_core
COPY cmd_demo.py benchmark.py ./

RUN useradd --create-home --shell /bin/bash appuser
USER appuser

CMD ["python", "-B", "cmd_demo.py"]

FROM python:3

WORKDIR /code
COPY . .

RUN mkdir -p results

RUN apt-get update \
    && apt-get install -y \
    libev4 \
    libev-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]

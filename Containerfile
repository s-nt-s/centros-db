FROM alpine:3.23.4

WORKDIR /app

COPY . .

RUN apk update
RUN apk add proj proj-dev proj-util
RUN apk add python3 python3-dev py3-pip
RUN apk add g++
RUN apk add poppler-dev


RUN python -m venv .venv
ENV PATH=".venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

CMD python build.py; python concurso.py


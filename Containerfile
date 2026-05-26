FROM alpine:3.23.4

WORKDIR /app

COPY . .

RUN apk add tzdata
RUN ln -s /usr/share/zoneinfo/Europe/Madrid /etc/localtime

RUN apk update
RUN apk add proj proj-dev proj-util
RUN apk add python3 python3-dev py3-pip
RUN apk add g++
RUN apk add poppler-dev


RUN python -m venv .venv
ENV PATH=".venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

CMD rm -f cache/etapas.json cache/form.json && rm -rf cache/csv/ && python dwn.py --centros --tcp-limit -50 && rm -rf cache/ids/ && python dwn.py --busquedas --tcp-limit -50 && python build.py --tcp-limit -50 && python readme.py && python area.py && python concurso.py



FROM alpine:latest AS builder
WORKDIR /vertex-orbital
COPY requirements.txt ./
COPY orbit.py ./
RUN apk update && apk add ---upgrade --no-cache apk-tools && apk upgrade --available
RUN apk add --update --no-cache python3 py3-pip
RUN python3 -m venv ./orbit
RUN source ./orbit/bin/activate && \
    python3 -m pip install --upgrade --no-cache-dir pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --no-cache-dir "psycopg[binary]" && \
    rm requirements.txt && \
    apk del py3-pip && \
    rm -rf /$HOME/.cache/pip3 && \
    apk upgrade -a

ENTRYPOINT ["./orbit/bin/python3", "orbit.py"]

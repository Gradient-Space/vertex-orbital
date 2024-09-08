FROM alpine:latest
WORKDIR /vertex-orbital
COPY requirements.txt ./
COPY orbit.py ./
RUN apk update && apk add --upgrade apk-tools && apk upgrade --available
RUN apk add --update --no-cache python3 py3-pip
RUN python3 -m venv ./orbit
RUN source ./orbit/bin/activate && \
    python3 -m pip install --upgrade pip && \
    pip3 install -r requirements.txt && \
    pip3 install "psycopg[binary]"

CMD ["./orbit/bin/python3", "orbit.py"]

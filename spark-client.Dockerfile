FROM spark-base

# -- Runtime

RUN apt-get update -y && \
    apt-get install -y vim

WORKDIR /opt/workspace

CMD tail -f /dev/null

FROM debian:11-slim
LABEL maintainer="Ariel S. Weher <ariel@weher.net>"
EXPOSE 8000
ENV DEBIAN_FRONTEND noninteractive
ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn
WORKDIR /app
COPY . .
RUN apt-get update && \
    apt-get install -y git python3 python3-pip tini && \
    pip3 install --upgrade pip && \
    pip3 install -r requirements.txt && \
    rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash", "start-debug.sh"]
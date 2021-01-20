FROM crystallang/crystal:0.35.1-alpine
WORKDIR /app

COPY shard.yml /app
RUN shards install

COPY spec /app/spec
COPY src /app/src

RUN crystal tool format --check

ENTRYPOINT ["crystal", "spec", "--error-trace", "-v"]
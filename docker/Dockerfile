# syntax=docker/dockerfile:1.7

ARG CADDY_VERSION=2.11.4

FROM caddy:${CADDY_VERSION}-builder-alpine AS builder

ARG CADDY_VERSION
ARG PGSTORE_VERSION=dev
COPY . /src/caddy-pgstore
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd /src/caddy-pgstore \
    && xcaddy build "v${CADDY_VERSION}" \
        --with github.com/c2hy/caddy-pgstore=/src/caddy-pgstore \
    && ./caddy list-modules | grep -Fx caddy.storage.postgres

FROM caddy:${CADDY_VERSION}-alpine

ARG PGSTORE_VERSION=dev
LABEL org.opencontainers.image.title="Caddy with caddy-pgstore" \
      org.opencontainers.image.version="${PGSTORE_VERSION}" \
      org.opencontainers.image.source="https://github.com/c2hy/caddy-pgstore"

COPY --from=builder /src/caddy-pgstore/caddy /usr/bin/caddy
COPY docker/entrypoint.sh /usr/bin/caddy-with-secrets
RUN chmod 0755 /usr/bin/caddy-with-secrets

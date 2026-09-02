#!/bin/sh
set -eu

password_file=/run/secrets/postgres_password

if [ ! -r "$password_file" ]; then
    echo "cannot read PostgreSQL password secret: $password_file" >&2
    exit 1
fi

: "${POSTGRES_HOST:=postgres}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=caddy}"
: "${POSTGRES_USER:=caddy}"

postgres_password=$(tr -d '\r\n' < "$password_file")
if [ -z "$postgres_password" ]; then
    echo "PostgreSQL password secret is empty" >&2
    exit 1
fi

export DATABASE_URL="postgresql://${POSTGRES_USER}:${postgres_password}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=disable"
unset postgres_password

exec /usr/bin/caddy "$@"

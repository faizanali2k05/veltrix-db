#!/usr/bin/env bash
set -euo pipefail

cli_bin="${REDIS_CLI:-redis-cli}"
server_host="${VELTRIX_HOST:-127.0.0.1}"
server_port="${VELTRIX_PORT:-6379}"

"$cli_bin" -h "$server_host" -p "$server_port" PING
"$cli_bin" -h "$server_host" -p "$server_port" SET demo value
"$cli_bin" -h "$server_host" -p "$server_port" GET demo
"$cli_bin" -h "$server_host" -p "$server_port" HSET user:1 name Veltrix role database
"$cli_bin" -h "$server_host" -p "$server_port" HGET user:1 role
"$cli_bin" -h "$server_host" -p "$server_port" SET fleeting yes PX 250
"$cli_bin" -h "$server_host" -p "$server_port" PTTL fleeting
sleep 0.35
"$cli_bin" -h "$server_host" -p "$server_port" GET fleeting
"$cli_bin" -h "$server_host" -p "$server_port" TTL fleeting

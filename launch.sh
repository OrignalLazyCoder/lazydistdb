#!/bin/bash
set -e

cd $(dirname $0)

go install -v

go run main.go -db-location=shard1.db -config-file=sharding.toml -shard=shard1 -http-addr=127.0.0.1:8080 &
go run main.go -db-location=shard1-replica.db -config-file=sharding.toml -shard=shard1 -http-addr=127.0.0.11:8080 -replica &
go run main.go -db-location=shard2.db -config-file=sharding.toml -shard=shard2 -http-addr=127.0.0.2:8080 &
go run main.go -db-location=shard2-replica.db -config-file=sharding.toml -shard=shard2 -http-addr=127.0.0.22:8080 -replica &
go run main.go -db-location=shard3.db -config-file=sharding.toml -shard=shard3 -http-addr=127.0.0.3:8080 &
go run main.go -db-location=shard3-replica.db -config-file=sharding.toml -shard=shard3 -http-addr=127.0.0.33:8080 -replica &
go run main.go -db-location=shard4.db -config-file=sharding.toml -shard=shard4 -http-addr=127.0.0.4:8080 &
go run main.go -db-location=shard4-replica.db -config-file=sharding.toml -shard=shard4 -http-addr=127.0.0.44:8080 -replica &

wait
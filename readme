# Lazy Dist DB

Lazy Dist DB is a Distributed layer written over bolt db in go lang to make the key value pair more reliable and scalable

## Features

- Key value pair database
- Read replica
- Sharding
- Distributed database

## Documentation

Here is the explaination of files that you will be needing to setup the data

### Sharding.toml

Contains configurations for different instances of databases that are connected to each other

- name: Name of Shard
- idx: index that you want to give to your database
- address: IP address accessible over public/private network
- replicas: Array of IP addresses that will be used for read only

### populate.sh

Script to seed database with random data for testing purpose

### launch.sh

Contains required commands to run the database

#### Commands

`go run main.go -db-location=shard1-replica.db -config-file=sharding.toml -shard=shard1 -http-addr=127.0.0.11:8080 -replica`

- ` go run main.go`: To run the script
- `-db-location=shard1-replica.db`: Path to database on the machine
- `-config-file=sharding.toml`: Path to config file
- `-shard=shard1`: Name of shard
- `http-addr=127.0.0.11:8080`: Address on which database will be accessible
- `-replica`: Flag to set if database instance is replica or not

## Run Locally

```bash
  ./launch.sh
```

## Authors

- [@lazycoderr - LinkedIn](linkedin.com/in/lazycoderr)
- [@lazycoderr - Github](https://github.com/OrignalLazyCoder)

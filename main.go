package main

import (
	"flag"
	"log"
	"net/http"

	config "github.com/originallazycoder/lazydistdb/config"
	db "github.com/originallazycoder/lazydistdb/db"
	replication "github.com/originallazycoder/lazydistdb/replication"
	utils "github.com/originallazycoder/lazydistdb/utils"
	web "github.com/originallazycoder/lazydistdb/web"
)

// Define flags that will be used for configuration of the database server
var (
	dbLocation = flag.String("db-location", "", "Path to the database. Should be on the same machine.")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Run for read only replica")
	migrate    = flag.Bool("migrate", false, "Flag used to migrate data")
	scale      = flag.String("scale", "up", "Is migration for up scaling or down scaling")
)

func parseFlags() {
	flag.Parse()

	if *migrate {
		if *replica {
			log.Fatalf("Cannot migrate a read-replica")
		}
		if !utils.Contains([]string{"up", "down"}, *scale) {
			log.Fatalf("Must provide valid scale value (up/down)")
		}
	}
	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	// Parse flags provided via CLI
	parseFlags()

	// parse TOML file
	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	// parse shard's configurations
	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	// initiate database
	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer close()

	// Added log for ease in debugging
	log.Printf("Started DB server -shard-id=%d -addr=http://%s. -replica=%v", shards.CurIdx, *httpAddr, *replica)

	// Validations and configurations for read replica only
	if *replica {
		masterAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Master Address not found for shard %d", shards.CurIdx)
		}
		go replication.ClientLoop(db, masterAddr)
	}

	if *migrate {
		if *scale == "up" {

		} else {

		}
		log.Printf("MIGRATION STARTED")
	} else {
		// initiate web server
		srv := web.NewServer(db, shards)

		// Define public end points
		http.HandleFunc("/get", srv.GetHandler)
		http.HandleFunc("/set", srv.SetHandler)
		http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
		http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
		http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)

		// listen web server on provided address
		log.Fatal(http.ListenAndServe(*httpAddr, nil))
	}
}

package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/originallazycoder/lazydistdb/db"
)

// Struct to store the key, value that will be replicated next
type NextKeyValue struct {
	Key   string
	Value string
	Err   error
}

// Struct to store replicata database instance information and masters address
type client struct {
	db         *db.Database
	masterAddr string
}

// ClientLoop continuously downloads new keys from the master and applies them.
func ClientLoop(db *db.Database, masterAddr string) {
	// instantiate client with master DB details
	c := &client{db: db, masterAddr: masterAddr}
	// never ednding loop to extract key, values for replication
	for {
		present, err := c.loop()
		// If error, pause execution for a second and resume iteration again
		if err != nil {
			log.Printf("Loop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// if no value is found, pause execution for 100 milliseconds and resume again
		if !present {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// loop on client
func (c *client) loop() (present bool, err error) {
	// get next key, value for replication
	resp, err := http.Get("http://" + c.masterAddr + "/next-replication-key")
	if err != nil {
		// return error if there is error in HTTP call
		return false, err
	}

	var res NextKeyValue
	// Decode key, value from response
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}
	defer resp.Body.Close()

	// If error found in network call, return error
	if res.Err != nil {
		return false, err
	}

	// If key is empty string, return no key found
	if res.Key == "" {
		return false, nil
	}

	// Set the replication key and value to the replica
	if err := c.db.SetKeyOnReplica(res.Key, []byte(res.Value)); err != nil {
		return false, err
	}

	// Delete the replicated key, value from the master's replication bucket to free storage
	if err := c.deleteFromReplicationQueue(res.Key, res.Value); err != nil {
		log.Printf("DeleteKeyFromReplication failed: %v", err)
	}

	return true, nil
}

// Send network request to delete key and value from master's replication bucket
func (c *client) deleteFromReplicationQueue(key, value string) error {
	u := url.Values{}
	// Set query parameters
	u.Set("key", key)
	u.Set("value", value)

	// Adding log to ease in debugging
	log.Printf("Deleting key=%q, value=%q from replication queue on %q", key, value, c.masterAddr)

	// Send network call to delete replicated key and value
	resp, err := http.Get("http://" + c.masterAddr + "/delete-replication-key?" + u.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response body
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Check for error and return if there is a error
	if !bytes.Equal(result, []byte("ok")) {
		return errors.New(string(result))
	}

	// return nil error
	return nil
}

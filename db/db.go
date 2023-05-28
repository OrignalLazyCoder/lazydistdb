package db

import (
	"bytes"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")
var replicaBucket = []byte("replication")

// Structure to store database instance and other configurations for database
type Database struct {
	db       *bolt.DB
	readOnly bool
}

// Returns new instance of database with a default bucket in boltdb with closing functions
func NewDatabase(dbPath string, readOnly bool) (db *Database, closeFunc func() error, err error) {
	// create/read the database from the database path
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		// If there is error in opening the database, return error
		return nil, nil, err
	}

	// populate Database struct
	db = &Database{db: boltDb, readOnly: readOnly}

	// Define closing function for proper closure of database
	closeFunc = boltDb.Close

	// Create the required buckets
	if err := db.createBuckets(); err != nil {
		// If there is error in creating the bucket, close the database and return error
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

// Create buckets
func (d *Database) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		// Create default bucket if not exists
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		// Create replication bucket if not exists - This will be temporary bucket only for replication purpose
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// Set value for a key provided
func (d *Database) SetKey(key string, value []byte) error {
	// If database is readonly, block write requests
	if d.readOnly {
		return errors.New("read-only mode")
	}

	// Initiate a transaction for writing in default and replication bucket
	return d.db.Update(func(tx *bolt.Tx) error {
		// Write value for the provided key
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}
		// Add a duplicaet in replication bucket
		return tx.Bucket(replicaBucket).Put([]byte(key), value)
	})
}

// Sets the key to the requested value into the default database and does not write to the replication queue.
// DANGER - USE ONLY ON READ REPLICA
func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

// Copy byte slices - Generic method to reduce duplicate code
func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// Get key and value for the keys that have changed and have not yet been applied to replicas.
// If there are no new keys, nil key and value will be returned.
func (d *Database) GetNextKeyForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		key = copyByteSlice(k)
		value = copyByteSlice(v)
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// Deletes the key from the replication queue
// if the value matches the contents or if the key is already absent.
func (d *Database) DeleteReplicationKey(key, value []byte) (err error) {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)

		v := b.Get(key)
		if v == nil {
			return errors.New("key does not exist")
		}

		if !bytes.Equal(v, value) {
			return errors.New("value does not match")
		}

		return b.Delete(key)
	})
}

// Get the value of the requested from a default database.
func (d *Database) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = copyByteSlice(b.Get([]byte(key)))
		return nil
	})

	if err == nil {
		return result, nil
	}
	return nil, err
}

// Deletes the keys that do not belong to this shard.
// DANGER - WILL HARD DELETE THE DATA. USE ONLY IF NEEDED AND YOU KNOW WHAT YOU ARE DOING
func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {

	// Get all the keys that don't belong to current shard
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		// iterate on keys
		return b.ForEach(func(k, v []byte) error {
			ks := string(k)
			// if key don't belong to the current shard, add to list
			if isExtra(ks) {
				keys = append(keys, ks)
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	// Hard delete the keys
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		// iterate on keys list
		for _, k := range keys {
			// Delete the keys
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

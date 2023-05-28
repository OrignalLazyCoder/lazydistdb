package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/originallazycoder/lazydistdb/config"
	"github.com/originallazycoder/lazydistdb/db"
	"github.com/originallazycoder/lazydistdb/replication"
)

//  Struct to define a web server that will interact with different shards and databases
type Server struct {
	db     *db.Database
	shards *config.Shards
}

// Create a new instance of the database server
func NewServer(db *db.Database, s *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

// This function is responsible for redirecting the request from one web server to another web server to get required response
func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	// Generate URL onto which request will be redirected
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurIdx, shard, url)

	// Send request to web server
	resp, err := http.Get(url)
	if err != nil {
		// Write an error stating that something went wrong. This can be written in a better way to provide more graceful errors
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v", err)
		return
	}
	defer resp.Body.Close()

	// copy response to http Response writer
	io.Copy(w, resp.Body)
}

// Get data from the database using this handler
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	// Get requested key from query params
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Index(key)

	// If provided key does not belong to requested database server, redirect it
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.GetKey(key)

	fmt.Fprintf(w, "Shard = %d, current shard = %d, addr = %q, Value = %q, error = %v", shard, s.shards.CurIdx, s.shards.Addrs[shard], value, err)
}

// Set a value for provided key using this handler
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	// Get requested key and value from query params
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	// If provided key does not belong to requested database server, redirect it
	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Error = %v, shardIdx = %d, current shard = %d, key = %s, value = %s", err, shard, s.shards.CurIdx, key, value)
}

// This is a special handler. This will delete all the keys that does not belong to the requested database server.
// DANGER - USE WITH CAUTION AS IT CAN LEAD TO DATALOSS IS CONFIGURED IN WRONG MANNER
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Index(key) != s.shards.CurIdx
	}))
}

// This function returns keys from the existing sharding shard for replication in read replica
func (s *Server) GetNextKeyForReplication(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	k, v, err := s.db.GetNextKeyForReplication()
	enc.Encode(&replication.NextKeyValue{
		Key:   string(k),
		Value: string(v),
		Err:   err,
	})
}

// This function deletes a copy of key and value from master that has been replicated
// DANGER - CALL ONLY AFTER THE KEY VALUE HAS BEEN REPLICATED. THIS CAN LEAD TO DATA LOSS
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.DeleteReplicationKey([]byte(key), []byte(value))
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}

	fmt.Fprintf(w, "ok")
}

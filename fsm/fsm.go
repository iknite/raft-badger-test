package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/iknite/raft-badger-test/encoding/encuint64"
	"github.com/iknite/raft-badger-test/hashing"
	"github.com/iknite/raft-badger-test/storage"
)

type fsmGenericResponse struct {
	error error
}

type fsmAddResponse struct {
	commitment []byte
	error      error
}

type FSM struct {
	hasher func() hashing.Hasher

	store   storage.ManagedStore
	version uint64

	restoreMu sync.RWMutex // Restore needs exclusive access to database.
}

func NewFSM(dbPath string, hasher func() hashing.Hasher) *FSM {
	return &FSM{
		hasher:  hasher,
		store:   storage.NewBadgerStore(dbPath),
		version: 0,
	}
}

// Apply applies a Raft log entry to the database.
func (fsm *FSM) Apply(l *raft.Log) interface{} {
	// TODO: should i use a restore mutex?

	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}

	switch cmd.Type {
	case insert:
		var sub insertSubCommand
		if err := json.Unmarshal(cmd.Sub, &sub); err != nil {
			return &fsmGenericResponse{error: err}
		}
		return fsm.applyAdd(sub.Key, sub.Value)
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", cmd.Type)}
	}
}

// Snapshot returns a snapshot of the key-value store. The caller must ensure that
// no Raft transaction is taking place during this call. Hashicorp Raft
// guarantees that this function will not be called concurrently with Apply.
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.restoreMu.Lock()
	defer fsm.restoreMu.Unlock()

	return &fsmSnapshot{store: fsm.store}, nil
}

// Restore stores the key-value store to a previous state.
func (fsm *FSM) Restore(rc io.ReadCloser) error {
	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	if err := fsm.store.Load(rc); err != nil {
		return err
	}

	// get stored last version
	kv, err := fsm.store.Get(storage.VersionPrefix, VersionKey)
	if err != nil {
		return err
	}
	fsm.version = encuint64.ToBytes(kv.Value) + 1

	return nil
}

func (fsm *FSM) Close() error {
	return fsm.store.Close()
}

func (fsm *FSM) applyAdd(key, value string) *fsmAddResponse {
	fsm.store.Mutate(storage.Mutation{storage.HistoryCachePrefix, key, value})
	return &fsmAddResponse{value, nil}
}

// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package request

import (
	"fmt"
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/tracing"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

var (
	executedEntriesChan chan *log.Entry // Channel to send executed entries to the responder
)

const (
	entryChannelCapacity = 10000
)

// Represents a Executor to client requests
type Executor struct {

	// Channel through which the log will push entries to the Executor in sequence number order.
	// The Executor reads from this channel and responds to the corresponding client for each entry.
	entriesChan chan *log.Entry

	kvStore map[int32]int32
	mu      sync.RWMutex
}

// Creates a new Executor.
// A Executor must be created before any protocol messages can be received from the network.
// Otherwise some responses to the client could be missed (in case entries are committed to the log before
// the Executor has been created).
func NewExecutor() *Executor {
	return &Executor{
		entriesChan: log.Entries(),
		kvStore:     make(map[int32]int32),
	}
}

// Creates and returns a new channel to which all the new log entries will be pushed in order.
func ExecutedEntries() chan *log.Entry {

	if executedEntriesChan == nil {
		executedEntriesChan = make(chan *log.Entry, entryChannelCapacity)
		logger.Info().
			Str("channel", "executedEntries").
			Msg("Creating new channel for executed entries.")
	}

	return executedEntriesChan
}

// Apply Execute Single Transaction (Put / Get)
func (r *Executor) Apply(req *pb.ClientRequest) (result string) {
	switch req.Op { // Check the operation type and execute accordingly
	case pb.Op_GET:
		r.mu.RLock()
		val, ok := r.kvStore[req.Key]
		r.mu.RUnlock()
		if ok {
			return fmt.Sprintf("GET(%d)=%d", req.Key, val)
		}
		return fmt.Sprintf("GET(%d)=<nil>", req.Key)

	default: // PUT
		r.mu.Lock()
		r.kvStore[req.Key] = req.Value
		r.mu.Unlock()
		return fmt.Sprintf("PUT(%d,%d)=OK", req.Key, req.Value)
	}
}

// Observes the log and responds to clients in commit order.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (r *Executor) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	// Read log entries (containing ordered batches) from
	// the entries channel until the channel is closed.
	for e := <-r.entriesChan; e != nil; e = <-r.entriesChan {

		// For each ClientRequest in the ordered batch
		for _, req := range e.Batch.Requests {
			result := r.Apply(req)

			logger.Debug().
				Int32("clientId", req.RequestId.ClientId).
				Int32("clientSn", req.RequestId.ClientSn).
				Int32("sn", e.Sn).
				Int32("Key", req.Key).
				Int32("Value", req.Value).
				Str("result", result).
				Msg("Executing the transaction.")

			// Respond to the corresponding client.
			tracing.MainTrace.Event(tracing.REQ_EXEC, int64(req.RequestId.ClientId), int64(req.RequestId.ClientSn))

			// messenger.RespondToClient(req.RequestId.ClientId, &pb.ClientResponse{
			// 	OrderSn:  e.Sn,
			// 	ClientSn: req.RequestId.ClientSn,
			// })
		}
		// Send the executed entry to the responder.
		executedEntriesChan <- e
		logger.Debug().
			Int32("sn", e.Sn).
			Msg("Sent executed entry to the responder.")
	}
}

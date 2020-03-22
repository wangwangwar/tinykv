package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage
	// used in 4A/4B
	Latches *latches.Latches
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your code here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)

	if err == badger.ErrKeyNotFound {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your code here (1).
	modify := []storage.Modify{storage.Modify{
		Type: storage.ModifyTypePut,
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), modify)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your code here (1).
	modify := []storage.Modify{storage.Modify{
		Type: storage.ModifyTypeDelete,
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), modify)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your code here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	kvPairs := []*kvrpcpb.KvPair{}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	var i uint32
	for {
		if !iter.Valid() || i >= req.GetLimit() {
			break
		}

		key := iter.Item().KeyCopy(nil)
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{Error: &kvrpcpb.KeyError{}})
		} else {
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}

		iter.Next()
		i++
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your code here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

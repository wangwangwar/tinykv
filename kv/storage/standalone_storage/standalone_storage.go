package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

// NewStandAloneStorage creates a new StandAloneStorage
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	subPath := "/test"
	db := engine_util.CreateDB(subPath, conf)
	return &StandAloneStorage{db: db}
}

// Start a StandAloneStorage
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

// Stop a StandAloneStorage
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

// Reader returns a Reader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &MyDBReader{s.db}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	batchWrite := new(engine_util.WriteBatch)
	for i := 0; i < len(batch); i++ {
		op := batch[i]
		if op.Type == storage.ModifyTypePut {
			batchWrite.SetCF(op.Cf(), op.Key(), op.Data.(storage.Put).Value)
		} else if op.Type == storage.ModifyTypeDelete {
			batchWrite.DeleteCF(op.Cf(), op.Key())
		}
	}

	return batchWrite.WriteToDB(s.db)
}

type MyDBReader struct {
	db *badger.DB
}

func (reader *MyDBReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(reader.db, cf, key)
}

func (reader *MyDBReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.db.NewTransaction(false))
}

func (reader *MyDBReader) Close() {
	reader.Close()
}

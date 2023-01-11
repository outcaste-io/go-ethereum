package fastdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/fastdb/store"
	"github.com/outcaste-io/badger/v4"
	"github.com/outcaste-io/lib/x"
	"github.com/pkg/errors"
)

// Fast would store data in log files. The files would contain a unique
// monotonically increasing ID, which has nothing to do with the block number.
// The block number would be mapped to this ID. So, each block number can
// correspond to multiple such IDs. However, the canonical ID would be marked as
// such either in BadgerDB index, or in the log files.
type Fast struct {
	db *badger.DB
	st *store.DiskStorage
}

func NewFastDB(path string) *Fast {
	fmt.Printf("opening new fast DB at: %s\n", path)
	opt := badger.DefaultOptions(path).WithNumVersionsToKeep(math.MaxInt64)
	db, err := badger.Open(opt)
	x.Check(err)

	st := store.Init(filepath.Join(path, "blocks"))
	return &Fast{db: db, st: st}
}

func (f *Fast) Close() error {
	err := f.st.Close()
	err2 := f.db.Close()
	if err != nil {
		return err
	}
	return err2
}

func (f *Fast) Compact(start []byte, limit []byte) error        { return nil }
func (f *Fast) Delete(key []byte) error                         { panic("don't expect delete") }
func (f *Fast) Has(key []byte) (bool, error)                    { panic("don't expect has") }
func (f *Fast) NewBatchWithSize(size int) ethdb.Batch           { return f.NewBatch() }
func (f *Fast) NewIterator(prefix, start []byte) ethdb.Iterator { return nil }
func (f *Fast) NewSnapshot() (ethdb.Snapshot, error)            { panic("dont' support this yet") }
func (f *Fast) Stat(property string) (string, error) {
	return "", errors.New("unknown property")
}

func (f *Fast) Put(key, val []byte) error {
	pk := common.ParseKey(key)
	fmt.Printf("Fast.Put: %+v | val: %d\n", pk, len(val))

	batch := f.db.NewWriteBatch()
	if err := batch.SetAt(key, val, 1); err != nil {
		return err
	}
	return batch.Flush()
}

func (f *Fast) Get(key []byte) ([]byte, error) {
	pk := common.ParseKey(key)
	fmt.Printf("Fast.Get: %+v\n", pk)

	switch pk.Type {
	case common.KeyBody, common.KeyHeader, common.KeyReceipts, common.KeyTotalDifficulty:
		fmt.Printf("PK: %+v | returning nil\n", pk)
		return nil, fmt.Errorf("not found")
	default:
		var ret []byte
		err := f.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			ret = val
			return nil
		})
		return ret, err
	}
}

var batchId uint64

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (fast *Fast) NewBatch() ethdb.Batch {
	bch := &batch{
		fast: fast,
		b:    fast.db.NewWriteBatch(),
	}
	bch.id = atomic.AddUint64(&batchId, 1)
	fmt.Printf("Created a new batch of id: %d\n", bch.id)
	return bch
}

type kv struct {
	key []byte
	val []byte
}

func newKV(key, value []byte) kv {
	var kv kv
	kv.key = append(kv.key, key...)
	kv.val = append(kv.val, value...)
	return kv
}

// type blockWriter struct {
// 	sync.Mutex
// 	db     *badger.DB
// 	blocks map[uint64]*block
// }

// func (bw *blockWriter) write(pk common.PK, val []byte) {
// 	bw.Lock()
// 	defer bw.Unlock()

// 	if pk.Type == common.KeyOther {
// 		panic("Invalid key passed")
// 	}
// }

const (
	// Please DO NOT change the number corresponding to the entries.
	// The only entry you can change here is IdxLast. Any new entries should be
	// between IdxTD and IdxLast.
	IdxHeader   int = 0
	IdxBody     int = 1
	IdxReceipts int = 2
	IdxTD       int = 3
	IdxLast     int = 4
)

type Block struct {
	number        uint64
	isDelete      bool
	hash          []byte
	canonicalHash []byte

	parts [][]byte
}

type SerBlock []byte

func (sb SerBlock) ToBlock() *Block {
	block := &Block{parts: make([][]byte, IdxLast)}
	var idx uint32
	for i := 0; ; i++ {
		sz := binary.BigEndian.Uint32(sb[idx:])
		idx += 4
		block.parts[i] = sb[idx : idx+sz]
		idx += sz
		if idx >= uint32(len(sb)) {
			break
		}
	}
	return block
}

func writeSlice(buf *bytes.Buffer, data []byte) {
	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(data)))
	x.Check2(buf.Write(sz[:]))
	x.Check2(buf.Write(data))
}

// ToSerBlock serializes the byte arrays for block structure. To remove a field,
// ensure that a nil byte slice is passed to ensure ordering of data.
func (b Block) ToSerBlock() SerBlock {
	buf := new(bytes.Buffer)
	for i := IdxHeader; i < IdxLast; i++ {
		writeSlice(buf, b.parts[i])
	}
	return SerBlock(buf.Bytes())
}

func (b *Block) Valid() bool {
	if b == nil {
		return false
	}
	for i := 0; i < len(b.parts); i++ {
		if len(b.parts[i]) == 0 {
			return false
		}
	}
	return true
}

var diskIndex uint64

func (b *Block) ToDisk(fast *Fast, idx uint64) error {
	sb := b.ToSerBlock()
	entry := store.Entry{Index: idx, Meta: 0, Number: b.number, Hash: b.hash, Data: sb}
	return fast.st.Save([]store.Entry{entry})
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	fast   *Fast
	b      *badger.WriteBatch
	blocks map[uint64]*Block
	id     uint64
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	pk := common.ParseKey(key)
	fmt.Printf("Fast Batch.PUT %d | %s | %+v | len(val): %d\n", b.id, pk.Type, pk, len(value))
	if pk.Type == common.KeyReceipts && pk.Number == 0 {
		panic("didn't expect Number to be zero")
	}
	if pk.Type < common.KeyOther && pk.Number == 0 {
		panic("didn't expect Number to be zero here")
	}

	var block *Block
	// We don't parse all the keys. In that case, number might be zero. And
	// therefore, block would be nil.
	if pk.Number > 0 {
		block = b.blocks[pk.Number]
		if block == nil {
			block = &Block{parts: make([][]byte, IdxLast)}
			b.blocks[pk.Number] = block
		}
		if len(pk.Hash) > 0 {
			block.hash = pk.Hash
		}
	}

	switch pk.Type {
	case common.KeyBody:
		block.parts[IdxBody] = value
	case common.KeyHeader:
		block.parts[IdxHeader] = value
	case common.KeyReceipts:
		block.parts[IdxReceipts] = value
	case common.KeyTotalDifficulty:
		block.parts[IdxTD] = value
	default:
		if err := b.b.SetAt(key, value, 1); err != nil {
			return errors.Wrapf(err, "while writing to Badger")
		}
	}
	fmt.Printf("block: %+v | valid: %v\n", block, block.Valid())
	if block.Valid() {
		block.number = pk.Number
		index := atomic.AddUint64(&diskIndex, 1)
		x.Check(block.ToDisk(b.fast, index))
		fmt.Printf("Written block to disk at %d | block: %+v\n", index, block)
	}
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	pk := common.ParseKey(key)
	fmt.Printf("Fast Batch.Delete %d | %+v\n", b.id, pk)
	if pk.Type >= common.KeyOther {
		if err := b.b.DeleteAt(key, 1); err != nil {
			return errors.Wrapf(err, "while writing to Badger")
		}
	}

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return 1 // Return a non-zero value, so we can determine the best way to flush the writes.
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	for num, block := range b.blocks {
		block.number = num
		if !block.Valid() {
			fmt.Printf("block is not valid: %+v\n", block)
		}
	}
	if err := b.b.Flush(); err != nil {
		return errors.Wrapf(err, "batch.Flush")
	}
	return nil
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b = b.fast.db.NewWriteBatch()
	b.id = atomic.AddUint64(&batchId, 1)
	fmt.Printf("Reset batch for id: %d\n", b.id)
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	// TODO: Figure out if we need this.
	return nil
}

// replayer is a small wrapper to implement the correct replay methods.
type replayer struct {
	writer  ethdb.KeyValueWriter
	failure error
}

// Put inserts the given value into the key-value data store.
func (r *replayer) Put(key, value []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the key-value data store.
func (r *replayer) Delete(key []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Delete(key)
}

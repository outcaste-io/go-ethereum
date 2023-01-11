/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package store

import (
	"math"
	"os"
	"sync"

	"github.com/golang/glog"
	"github.com/outcaste-io/lib/x"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"golang.org/x/net/trace"
)

// versionKey is hardcoded into the special key used to fetch the maximum version from the DB.
const versionKey = 1

// DiskStorage handles disk access and writing for the RAFT write-ahead log.
// Dir contains wal.meta file and <start idx zero padded>.wal files.
//
// === wal.meta file ===
// This file is generally around 4KB, so it can fit nicely in one Linux page.
//
//   Layout:
// 00-08 Bytes: Raft ID
// 08-16 Bytes: Group ID
// 16-24 Bytes: Checkpoint Index
// 512 Bytes: Hard State (Marshalled)
// 1024-1032 Bytes: Snapshot Index
// 1032-1040 Bytes: Snapshot Term
// 1040 Bytes: Snapshot (Marshalled)
//
// --- <0000i>.wal files ---
// These files contain raftpb.Entry protos. Each entry is composed of term, index, type and data.
//
// Term takes 8 bytes. Index takes 8 bytes. Type takes 8 bytes. And for data, we store an offset to
// the actual slice, which is 8 bytes. Size of entry = 32 bytes.
// First 30K entries would consume 960KB, hence fitting on the first MB of the file (logFileOffset).
//
// Pre-allocate 1MB in each file just for these entries, and zero them out explicitly. Zeroing them
// out ensures that we know when these entries end, in case of a restart.
//
// And the data for these entries are laid out starting logFileOffset. Those are the offsets you
// store in the Entry for Data field.
// After 30K entries, we rotate the file.
//
// --- clean up ---
// If snapshot idx = Idx_s. We find the first log file whose first entry is
// less than Idx_s. This file and anything above MUST be kept. All the log
// files lower than this file can be deleted.
//
// --- sync ---
// mmap fares well with process crashes without doing anything. In case
// HardSync is set, msync is called after every write, which flushes those
// writes to disk.
type DiskStorage struct {
	dir  string
	elog trace.EventLog

	meta *metaFile
	wal  *wal
	lock sync.Mutex
}

// Init initializes an instance of DiskStorage without encryption.
func Init(dir string) *DiskStorage {
	os.MkdirAll(dir, 0775)
	ds, err := initStorage(dir)
	x.Check(err)
	return ds
}

// InitEncrypted initializes returns a properly initialized instance of DiskStorage.
// To gracefully shutdown DiskStorage, store.Closer.SignalAndWait() should be called.
func initStorage(dir string) (*DiskStorage, error) {
	w := &DiskStorage{
		dir: dir,
	}

	var err error
	if w.meta, err = newMetaFile(dir); err != nil {
		return nil, err
	}
	// fmt.Printf("meta: %s\n", heDump(w.meta.data[1024:2048]))
	// fmt.Printf("found snapshot of size: %d\n", sliceSize(w.meta.data, snapshotOffset))

	if w.wal, err = openWal(dir); err != nil {
		return nil, err
	}

	w.elog = trace.NewEventLog("Badger", "RaftStorage")

	first, _ := w.FirstIndex()

	// If db is not closed properly, there might be index ranges for which delete entries are not
	// inserted. So insert delete entries for those ranges starting from 0 to (first-1).
	last := w.wal.LastIndex()

	glog.Infof("Init Raft Storage with first: %d, last: %d\n",
		first, last)
	return w, nil
}

func (w *DiskStorage) Path() string                     { return w.dir }
func (w *DiskStorage) SetUint(info MetaInfo, id uint64) { w.meta.SetUint(info, id) }
func (w *DiskStorage) Uint(info MetaInfo) uint64        { return w.meta.Uint(info) }

// Checkpoint returns the Raft index corresponding to the checkpoint.
func (w *DiskStorage) Checkpoint() (uint64, error) {
	if w.meta == nil {
		return 0, errors.Errorf("uninitialized meta file")
	}
	return w.meta.Uint(CheckpointIndex), nil
}

// Implement the Raft.Storage interface.
// -------------------------------------

func (w *DiskStorage) NumEntries() int {
	w.lock.Lock()
	defer w.lock.Unlock()

	start := w.wal.firstIndex()

	var count int
	for {
		ents := w.wal.allEntries(start, math.MaxUint64, 64<<20)
		if len(ents) == 0 {
			return count
		}
		count += len(ents)
		start = ents[len(ents)-1].Index + 1
	}
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (w *DiskStorage) Entries(lo, hi, maxSize uint64) (es []Entry, rerr error) {
	// glog.Infof("Entries: [%d, %d) maxSize:%d", lo, hi, maxSize)
	w.lock.Lock()
	defer w.lock.Unlock()

	// glog.Infof("Entries after lock: [%d, %d) maxSize:%d", lo, hi, maxSize)

	first := w.wal.firstIndex()
	if lo < first {
		glog.Errorf("lo: %d <first: %d\n", lo, first)
		return nil, raft.ErrCompacted
	}

	last := w.wal.LastIndex()
	if hi > last+1 {
		glog.Errorf("hi: %d > last+1: %d\n", hi, last+1)
		return nil, raft.ErrUnavailable
	}

	ents := w.wal.allEntries(lo, hi, maxSize)
	// glog.Infof("got entries [%d, %d): %+v\n", lo, hi, ents)
	return ents, nil
}

func (w *DiskStorage) Term(idx uint64) (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	si := w.meta.Uint(SnapshotIndex)
	if idx < si {
		glog.Errorf("TERM for %d = %v\n", idx, raft.ErrCompacted)
		return 0, raft.ErrCompacted
	}
	if idx == si {
		return w.meta.Uint(SnapshotTerm), nil
	}

	term, err := w.wal.Term(idx)
	if err != nil {
		glog.Errorf("TERM for %d = %v\n", idx, err)
	}
	// glog.Errorf("Got term: %d for index: %d\n", term, idx)
	return term, err
}

func (w *DiskStorage) LastIndex() (RaftIndex, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	li := w.wal.LastIndex()
	si := w.meta.Uint(SnapshotIndex)
	if li < si {
		return RaftIndex(si), nil
	}
	return RaftIndex(li), nil
}

func (w *DiskStorage) firstIndex() uint64 {
	if si := w.Uint(SnapshotIndex); si > 0 {
		return si + 1
	}
	return w.wal.firstIndex()
}

// FirstIndex returns the first inde It is typically SnapshotIndex+1.
func (w *DiskStorage) FirstIndex() (RaftIndex, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	return RaftIndex(w.firstIndex()), nil
}

// Save would write Entries, HardState and Snapshot to persistent storage in order, i.e. Entries
// first, then HardState and Snapshot if they are not empty. If persistent storage supports atomic
// writes then all of them can be written together. Note that when writing an Entry with Index i,
// any previously-persisted entries with Index >= i must be discarded.
func (w *DiskStorage) Save(es []Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.wal.AddEntries(es); err != nil {
		return err
	}
	return nil
}

// Append the new entries to storage.
func (w *DiskStorage) addEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	firste := entries[0].Index
	if firste+uint64(len(entries))-1 < first.U() {
		// All of these entries have already been compacted.
		return nil
	}
	if first.U() > firste {
		// Truncate compacted entries
		entries = entries[first.U()-firste:]
	}

	// AddEntries would zero out all the entries starting entries[0].Index before writing.
	if err := w.wal.AddEntries(entries); err != nil {
		return errors.Wrapf(err, "while adding entries")
	}
	return nil
}

func (w *DiskStorage) NumLogFiles() int {
	return len(w.wal.files)
}

// Sync calls the Sync method in the underlying badger instance to write all the contents to disk.
func (w *DiskStorage) Sync() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.meta.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing meta")
	}
	if err := w.wal.current.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing current file")
	}
	return nil
}

// Close closes the DiskStorage.
func (w *DiskStorage) Close() error {
	return w.Sync()
}

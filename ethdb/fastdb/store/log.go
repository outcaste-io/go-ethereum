/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/outcaste-io/lib/x"
	"github.com/outcaste-io/lib/y"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

// WAL is divided up into entryFiles. Each entry file stores maxNumEntries in
// the first logFileOffset bytes. Each entry takes a fixed entrySize bytes of
// space. The variable length data for these entries is written after
// logFileOffset from file beginning. Once snapshot is taken, all the files
// containing entries below snapshot index are deleted.

const (
	// maxNumEntries is maximum number of entries before rotating the file.
	maxNumEntries = 30000
	// logFileOffset is offset in the log file where data is stored.
	logFileOffset = 2 << 20 // 2MB
	// encOffset is offset in the log file where keyID (first 8 bytes)
	// and baseIV (remaining 8 bytes) are stored.
	encOffset = logFileOffset - 16 // 2MB - 16B
	// logFileSize is the initial size of the log file.
	logFileSize = 256 << 20 // 256MB
	// entryHeaderSize is the size in bytes of a single entry.
	entryHeaderSize = 64
	// logSuffix is the suffix for log files.
	logSuffix = ".wal"
)

var (
	emptyEntry = entryHeader(make([]byte, entryHeaderSize))
)

type entryHeader []byte

func (e entryHeader) Index() uint64      { return binary.BigEndian.Uint64(e) }
func (e entryHeader) Meta() uint64       { return binary.BigEndian.Uint64(e[8:]) }
func (e entryHeader) Number() uint64     { return binary.BigEndian.Uint64(e[16:]) }
func (e entryHeader) DataOffset() uint64 { return binary.BigEndian.Uint64(e[24:]) }
func (e entryHeader) Hash() []byte       { return y.Copy(e[32:64]) }

func marshalHeader(dst []byte, entry Entry, offset uint64) {
	x.AssertTrue(len(dst) == entryHeaderSize)

	binary.BigEndian.PutUint64(dst, entry.Index)
	binary.BigEndian.PutUint64(dst[8:], entry.Meta)
	binary.BigEndian.PutUint64(dst[16:], entry.Number)
	binary.BigEndian.PutUint64(dst[24:], offset)
	x.AssertTrue(32 == copy(dst[32:], entry.Hash[:]))
}

// logFile represents a single log file.
type logFile struct {
	*z.MmapFile
	fid int64
}

func logFname(dir string, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", id, logSuffix))
}

// openLogFile opens a logFile in the given directory. The filename is
// constructed based on the value of fid.
func openLogFile(dir string, fid int64) (*logFile, error) {
	glog.V(3).Infof("opening log file: %d\n", fid)
	fpath := logFname(dir, fid)
	lf := &logFile{
		fid: fid,
	}
	var err error
	// Open the file in read-write mode and create it if it doesn't exist yet.
	lf.MmapFile, err = z.OpenMmapFile(fpath, os.O_RDWR|os.O_CREATE, logFileSize)

	if err == z.NewFile {
		glog.V(3).Infof("New file: %d\n", fid)
		z.ZeroOut(lf.Data, 0, logFileOffset)

	} else if err != nil {
		x.Check(err)
	} else {
		buf := lf.Data[encOffset : encOffset+16]

		// The following is for encryption, that we're not using here.
		_ = binary.BigEndian.Uint64(buf[:8])
	}
	return lf, nil
}

// getHeader gets the entry at the slot id
func (lf *logFile) getHeader(idx int) entryHeader {
	if lf == nil {
		return emptyEntry
	}
	x.AssertTrue(idx < maxNumEntries)
	offset := idx * entryHeaderSize
	return entryHeader(lf.Data[offset : offset+entryHeaderSize])
}

// GetEntry gets the entry at the index idx, reads the data from the appropriate
// offset and converts it to a raftpb.Entry object.
func (lf *logFile) GetEntry(idx int) Entry {
	entry := lf.getHeader(idx)
	re := Entry{
		Index:  entry.Index(),
		Meta:   entry.Meta(),
		Number: entry.Number(),
		Hash:   entry.Hash(),
	}
	if entry.Number() > 0 {
		x.AssertTrue(entry.Number() < uint64(len(lf.Data)))
		data := lf.Slice(int(entry.Number()))
		if len(data) > 0 {
			// Copy the data over to allow the mmaped file to be deleted later.
			re.Data = append(re.Data, data...)
		}
	}
	return re
}

// firstIndex returns the first index in the file.
func (lf *logFile) firstIndex() uint64 {
	return lf.getHeader(0).Index()
}

// firstEmptySlot returns the index of the first empty slot in the file.
func (lf *logFile) firstEmptySlot() int {
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getHeader(i)
		return e.Index() == 0
	})
}

// lastEntry returns the last valid entry in the file.
func (lf *logFile) lastEntry() entryHeader {
	// This would return the first pos, where e.Index() == 0.
	pos := lf.firstEmptySlot()
	if pos > 0 {
		pos--
	}
	return lf.getHeader(pos)
}

// slotGe would return -1 if raftIndex < firstIndex in this file.
// Would return maxNumEntries if raftIndex > lastIndex in this file.
// If raftIndex is found, or the entryFile has empty slots, the offset would be between
// [0, maxNumEntries).
func (lf *logFile) slotGe(raftIndex uint64) int {
	fi := lf.firstIndex()
	// If first index is zero or the first index is less than raftIndex, this
	// raftindex should be in a previous file.
	if fi == 0 || raftIndex < fi {
		return -1
	}

	// Look at the entry at slot diff. If the log has entries for all indices between
	// fi and raftIndex without any gaps, the entry should be there. This is an
	// optimization to avoid having to perform the search below.
	if diff := int(raftIndex - fi); diff < maxNumEntries && diff >= 0 {
		e := lf.getHeader(diff)
		if e.Index() == raftIndex {
			return diff
		}
	}

	// Find the first entry which has in index >= to raftInde
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getHeader(i)
		if e.Index() == 0 {
			// We reached too far to the right and found an empty slot.
			return true
		}
		return e.Index() >= raftIndex
	})
}

// WalkPathFunc walks the directory 'dir' and collects all path names matched by
// func f. If the path is a directory, it will set the bool argument to true.
// Returns empty string slice if nothing found, otherwise returns all matched path names.
func WalkPathFunc(dir string, f func(string, bool) bool) []string {
	var list []string
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f(path, fi.IsDir()) {
			list = append(list, path)
		}
		return nil
	})
	if err != nil {
		glog.Errorf("Error while scanning %q: %s", dir, err)
	}
	return list
}

// getLogFiles returns all the log files in the directory sorted by the first
// index in each file.
func getLogFiles(dir string) ([]*logFile, error) {
	entryFiles := WalkPathFunc(dir, func(path string, isDir bool) bool {
		if isDir {
			return false
		}
		if strings.HasSuffix(path, logSuffix) {
			return true
		}
		return false
	})

	var files []*logFile
	seen := make(map[int64]struct{})

	for _, fpath := range entryFiles {
		_, fname := filepath.Split(fpath)
		fname = strings.TrimSuffix(fname, logSuffix)

		fid, err := strconv.ParseInt(fname, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "while parsing: %s", fpath)
		}

		if _, ok := seen[fid]; ok {
			glog.Fatalf("Entry file with id: %d is repeated", fid)
		}
		seen[fid] = struct{}{}

		f, err := openLogFile(dir, fid)
		if err != nil {
			return nil, err
		}
		glog.Infof("Found file: %d First Index: %d\n", fid, f.firstIndex())
		files = append(files, f)
	}

	// Sort files by the first index they store.
	sort.Slice(files, func(i, j int) bool {
		return files[i].getHeader(0).Index() < files[j].getHeader(0).Index()
	})
	return files, nil
}

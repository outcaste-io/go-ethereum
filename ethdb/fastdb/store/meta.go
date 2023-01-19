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

	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

type MetaInfo int

const (
	RaftId MetaInfo = iota
	GroupId
	CheckpointIndex
	SnapshotIndex
	SnapshotTerm
)

// getOffset returns offsets in wal.meta file.
func getOffset(info MetaInfo) int {
	switch info {
	case RaftId:
		return 0
	case GroupId:
		return 8
	case CheckpointIndex:
		return 16
	case SnapshotIndex:
		return snapshotIndex
	case SnapshotTerm:
		return snapshotIndex + 8
	default:
		panic("Invalid info: " + fmt.Sprint(info))
	}
}

// Constants to use when writing to mmap'ed meta and entry files.
const (
	// metaName is the name of the file used to store metadata (e.g raft ID, checkpoint).
	metaName = "wal.meta"
	// metaFileSize is the size of the wal.meta file.
	metaFileSize = 1 << 20
	//hardStateOffset is the offset of the hard sate within the wal.meta file.
	hardStateOffset = 512
	// snapshotIndex stores the index and term corresponding to the snapshot.
	snapshotIndex = 1024
	// snapshotOffest is the offset of the snapshot within the wal.meta file.
	snapshotOffset = snapshotIndex + 16
)

func readSlice(dst []byte, offset int) []byte {
	b := dst[offset:]
	sz := binary.BigEndian.Uint32(b)
	return b[4 : 4+sz]
}
func writeSlice(dst []byte, src []byte) {
	binary.BigEndian.PutUint32(dst[:4], uint32(len(src)))
	copy(dst[4:], src)
}
func sliceSize(dst []byte, offset int) int {
	sz := binary.BigEndian.Uint32(dst[offset:])
	return 4 + int(sz)
}

// metaFile stores the RAFT metadata (e.g RAFT ID, snapshot, hard state).
type metaFile struct {
	*z.MmapFile
}

// newMetaFile opens the meta file in the given directory.
func newMetaFile(dir string) (*metaFile, error) {
	fname := filepath.Join(dir, metaName)
	// Open the file in read-write mode and creates it if it doesn't exist.
	mf, err := z.OpenMmapFile(fname, os.O_RDWR|os.O_CREATE, metaFileSize)
	if err == z.NewFile {
		z.ZeroOut(mf.Data, 0, snapshotOffset+4)
	} else if err != nil {
		return nil, errors.Wrapf(err, "unable to open meta file")
	}
	return &metaFile{MmapFile: mf}, nil
}

func (m *metaFile) bufAt(info MetaInfo) []byte {
	pos := getOffset(info)
	return m.Data[pos : pos+8]
}
func (m *metaFile) Uint(info MetaInfo) uint64 {
	return binary.BigEndian.Uint64(m.bufAt(info))
}
func (m *metaFile) SetUint(info MetaInfo, id uint64) {
	binary.BigEndian.PutUint64(m.bufAt(info), id)
}
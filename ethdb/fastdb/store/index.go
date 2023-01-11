package store

type RaftIndex uint64
type BlockNumber uint64

func (ri RaftIndex) Block() BlockNumber {
	return BlockNumber(uint64(ri) - 1)
}
func (ri RaftIndex) U() uint64 {
	return uint64(ri)
}

func (bn BlockNumber) Raft() RaftIndex {
	return RaftIndex(uint64(bn) + 1)
}
func (bn BlockNumber) U() uint64 {
	return uint64(bn)
}

type Entry struct {
	Index  uint64
	Meta   uint64
	Number uint64
	Hash   []byte
	Data   []byte
}

func (e Entry) Size() int {
	// All the other fields are stored within entryHeader.
	return len(e.Data)
}

package common

type KeyType int

func (kt KeyType) String() string {
	all := []string{"KeyHeader", "KeyHeaderNumber", "KeyTxn", "KeyBody",
		"KeyReceipts", "KeyTotalDifficulty", "KeyCanonicalHash", "KeyOther",
		"KeySnapshot", "KeySnapshotAccount", "KeySnapshotStorage", "KeySkeleton"}
	return all[int(kt)]
}

// Please keep KeyType.String function insync with this.
const (
	KeyHeader KeyType = iota
	KeyHeaderNumber
	KeyTxn
	KeyBody
	KeyReceipts
	KeyTotalDifficulty
	KeyCanonicalHash
	KeyOther // Act as a divider for keys which aren't being processed to storage.
	KeySnapshot
	KeySnapshotAccount
	KeySnapshotStorage
	KeySkeleton
)

type PK struct {
	Type        KeyType
	Number      uint64
	Hash        []byte
	StorageHash []byte
	KeyStr      string
}

var ParseKey func(key []byte) *PK

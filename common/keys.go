package common

type KeyType int

func (kt KeyType) String() string {
	all := []string{"KeyHeader", "KeyHeaderNumber", "KeyTxn", "KeySnapshot",
		"KeySnapshotAccount", "KeySnapshotStorage", "KeyBody", "KeyReceipts",
		"KeySkeleton", "KeyTotalDifficulty", "KeyCanonicalHash", "KeyUnableToParse"}
	return all[int(kt)]
}

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

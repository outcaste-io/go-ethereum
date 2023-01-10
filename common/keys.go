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
	KeySnapshot
	KeySnapshotAccount
	KeySnapshotStorage // 5
	KeyBody
	KeyReceipts
	KeySkeleton
	KeyTotalDifficulty
	KeyCanonicalHash // 10
	KeyUnableToParse
)

type PK struct {
	Type        KeyType
	Number      uint64
	Hash        []byte
	StorageHash []byte
	KeyStr      string
}

var ParseKey func(key []byte) *PK

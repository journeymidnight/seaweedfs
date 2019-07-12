package needle

type Version uint8

const (
	Version1       = Version(1)
	Version2       = Version(2)
	Version3       = Version(3)
	Version4       = Version(4) // cannlys 2.1 kv store
	CurrentVersion = Version4
)

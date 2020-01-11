package pb

type (
	DataKey struct {
		KeyId     uint64
		Data      []byte
		Iv        []byte
		CreatedAt int64
	}
)

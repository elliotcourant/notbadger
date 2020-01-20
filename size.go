package notbadger

type (
	databaseSize struct {
		// LSMSize stores the size of the LSM tree in bytes.
		LSMSize int64

		// ValueLogSize stores the size of the value log in bytes.
		ValueLogSize int64
	}
)

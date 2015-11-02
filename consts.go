package themis

var (
	PutFamilyName  = []byte("#p")
	DelFamilyName  = []byte("#d")
	LockFamilyName = []byte("L")
)

const (
	ThemisServiceName string = "ThemisService"
	// when lock conflict, try count for clear lock and prewrite
	clearLockAndPrewriteTryCount = 2
)

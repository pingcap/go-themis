package themis

// Hooks for debugging and testing
type fnHook func(txn *themisTxn, ctx interface{}) (bypass bool, ret interface{}, err error)

type hookPoint int

const (
	hookAfterChoosePrimary hookPoint = iota
	hookAfterChooseSecondary
	hookBeforePrewritePrimary
	hookBeforePrewriteLockClean
	hookBeforePrewriteSecondary
	hookBeforeCommitPrimary
	hookBeforeCommitSecondary
	hookOnSecondaryOccursLock
	hookOnPrewriteRow
	hookOnTxnSuccess
	hookOnTxnFailed
)

type txnHook map[hookPoint]fnHook

func newHook() txnHook {
	return make(txnHook)
}

func (h txnHook) addPoint(point hookPoint, fn fnHook) {
	h[point] = fn
}

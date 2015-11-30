package themis

// Hooks for debugging and testing
type fnHook func(txn *themisTxn, ctx interface{}) (bypass bool, ret interface{}, err error)

type txnHook struct {
	afterChoosePrimaryAndSecondary fnHook
	beforePrewritePrimary          fnHook
	beforePrewriteLockClean        fnHook
	beforePrewriteSecondary        fnHook
	beforeCommitPrimary            fnHook
	beforeCommitSecondary          fnHook
	onSecondaryOccursLock          fnHook
	onPrewriteRow                  fnHook
	onTxnSuccess                   fnHook
	onTxnFailed                    fnHook
}

func newHook() *txnHook {
	return &txnHook{}
}

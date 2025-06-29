package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const FREE_STATE = "X"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id        string
	lockValue string
	version   rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, id: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	lockValue := kvtest.RandValue(8)

	for {
		currentLockValue, currentLockVersion, getStatus := lk.ck.Get(lk.id)

		switch getStatus {
		case rpc.ErrNoKey:
			{
				// Lock is available
				// To grab it we should put our `lockValue` with version = 0
				if lk.updateLock(lockValue, 0) {
					lk.lockValue = lockValue
					lk.version = 1
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
		case rpc.OK:
			{
				// "X" is a free lock state which can be claimed by others
				// Free Lock state -> Try to acquire it with currentVersion
				if currentLockValue == FREE_STATE && lk.updateLock(lockValue, currentLockVersion) {
					lk.lockValue = lockValue
					lk.version = currentLockVersion + 1
					return
				}
				// Lock is already acquired by someone else
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	// We can directly do a PUT call with lock's current version
	// If version is 0 that means we do not own the lock
	if lk.version == 0 {
		return
	}

	if lk.updateLock(FREE_STATE, lk.version) {
		// Lock released successfully
	}
}

func (lk *Lock) updateLock(lockValue string, version rpc.Tversion) bool {
	putStatus := lk.ck.Put(lk.id, lockValue, version)

	switch putStatus {
	case rpc.ErrVersion:
		{
			// Someone else acquired the lock before us
			return false
		}
	case rpc.OK:
		{
			//Lock acquistion successful with our lock value and version
			return true
		}
	default:
		{
			// With network being unreliable possibility of rpc.ErrMaybe
			return false
		}
	}
}

package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key       string
	identifer string
	ver       rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	id := kvtest.RandValue(8)
	lk := &Lock{ck: ck, key: l, identifer: id, ver: 0}

	_, _, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey {
		for {
			err = lk.ck.Put(lk.key, "unlocked", 0)
			if err == rpc.OK {
				break
			} else {
				_, _, err = lk.ck.Get(lk.key)
				if err == rpc.OK {
					break
				}
			}
		}
	}
	return lk
}

// The timer function is used to detect timeouts in any function that
// might block indefinitely. You can use it by adding the code below
// to the start of any function.
// 		ch := make(chan struct{})
// 		go timer(ch, "function name", duration(int))
// 		defer func() { ch <- struct{}{} }()
/* func timer(ch chan struct{}, pos string, t int32) {
	// This function is not used in the lock implementation.
	// It can be used for debugging or testing purposes.
	select {
	case <-ch:
		return // timer expired
	case <-time.After(t * time.Second):
		log.Printf("Lock %s: timer did not expire", pos)
	}
} */

func (lk *Lock) Acquire() {
	// Your code here

	for {
		state, version, err := lk.ck.Get(lk.key)
		switch err {
		case rpc.ErrNoKey:
			log.Fatal("Lock.Acquire: key does not exist")
		case rpc.OK:
			switch state {
			case "unlocked":
				err = lk.ck.Put(lk.key, lk.identifer, version)
				switch err {
				case rpc.OK:
					lk.ver = version + 1
					return // lock acquired successfully
				case rpc.ErrMaybe:
					state, version, _ = lk.ck.Get(lk.key)
					if state == lk.identifer {
						lk.ver = version
						return
					}

				}
			case lk.identifer:
				return
			}
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func (lk *Lock) Release() {
	// Your code here

	if lk.ver == 0 {
		return // lock is mot held by this client
	}
	err := lk.ck.Put(lk.key, "unlocked", lk.ver)
	retry := false
	for err != rpc.OK {
		if err == rpc.ErrMaybe {
			time.Sleep(time.Millisecond * 50)
			err = lk.ck.Put(lk.key, "unlocked", lk.ver)
			retry = true
		} else if retry {
			break // lock has been released
		} else {
			log.Fatalf("Lock.Release: unexpected error %v", err)
		}
	}
	lk.ver = 0
}

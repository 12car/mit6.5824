package util

import (
	"math/rand"
	"sync"
	"testing"
)

func TestSyncList_Push(t *testing.T) {
	l := MakeSyncList[int]()
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		for i := 0; i < 100; i++ {
			l.Push(int(rand.Int31n(1 << 16)))
		}

		wg.Done()
	}()

	go func() {
		for i := 0; i < 100; i++ {
			l.Push(int(rand.Int31n(1 << 16)))
		}

		wg.Done()
	}()

	wg.Wait()

	if l.Len() != 200 {
		t.Fatalf("插入量不正确, Len: %d, list: %v\n", l.Len(), l.list)
	}
}

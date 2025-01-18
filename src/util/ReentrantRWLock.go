package util

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type ReentrantRWLock struct {
	sync.Locker
	goroutineId    atomic.Int64
	reentrantTimes atomic.Int32
}

func MakeReentrantLock() *ReentrantRWLock {
	return &ReentrantRWLock{
		Locker:         new(sync.RWMutex),
		goroutineId:    atomic.Int64{},
		reentrantTimes: atomic.Int32{},
	}
}

func (rwLock *ReentrantRWLock) Lock() {
	id := GoID()
	// 可重入 增加重入计数并返回
	if rwLock.goroutineId.Load() == id {
		rwLock.reentrantTimes.Add(1)
		return
	}

	rwLock.Locker.Lock()

	// 协程第一次获取锁 保存协程id 并将重入计数置为1
	rwLock.goroutineId.Store(id)
	rwLock.reentrantTimes.Store(1)
}

func (rwLock *ReentrantRWLock) Unlock() {
	id := GoID()
	if rwLock.goroutineId.Load() != id {
		errMsg := fmt.Sprintf("当前协程{%d}不能释放其他协程{%d}持有的锁", id, rwLock.goroutineId.Load())
		panic(errMsg)
	}

	rwLock.reentrantTimes.Add(-1)

	// 如果可重入计数没有减少到0 那么说明不是真正释放锁
	if rwLock.reentrantTimes.Load() > 0 {
		return
	} else if rwLock.reentrantTimes.Load() < 0 {
		var buf [1024]byte
		n := runtime.Stack(buf[:], false)
		log.Printf("%v", buf[:n])
		panic("unlock次数超过lock次数")
	}

	// 将持有锁的协程id置为空
	rwLock.goroutineId.Store(-1)
	// 真正释放锁
	rwLock.Locker.Unlock()
}

func GoID() int64 {
	var buf [64]byte
	/**
		获取当前栈帧信息, 以下是示例:
		goroutine 18 [running]:
		main.GoID()
	        /Users/zixuan4/Desktop/proj
	*/
	n := runtime.Stack(buf[:], false)
	// n为写入buf的字节数 下面一行表示从buf中取出写入的n字节，转为字符串后删掉前缀goroutine
	trimStr := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	// 此时 将上一步结果按空格拆分为string数组 数组中第一个元素就是goroutine id
	idField := strings.Fields(trimStr)[0]
	id, _ := strconv.ParseInt(idField, 10, 64)
	return id
}

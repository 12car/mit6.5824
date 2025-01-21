package util

import (
	"container/list"
	"fmt"
	"strings"
)

type T any

type SyncList[t T] struct {
	*ReentrantRWLock
	list *list.List
}

func MakeSyncList[t T]() *SyncList[t] {
	l := new(list.List)
	l.Init()
	return &SyncList[t]{
		ReentrantRWLock: MakeReentrantLock(),
		list:            l,
	}
}

func (sl *SyncList[t]) Push(value T) {
	sl.Lock()
	defer sl.Unlock()
	sl.list.PushBack(value)
}

func (sl *SyncList[t]) Poll() T {
	sl.Lock()
	defer sl.Unlock()
	if sl.list.Len() == 0 {
		return nil
	}
	e := sl.list.Front()
	return sl.list.Remove(e).(T)
}

func (sl *SyncList[t]) peek() T {
	sl.Lock()
	defer sl.Unlock()
	if sl.list.Len() == 0 {
		return nil
	}
	return sl.list.Front().Value.(T)
}

func (sl *SyncList[t]) Len() int {
	sl.Lock()
	defer sl.Unlock()
	return sl.list.Len()
}

func (sl *SyncList[t]) Peek() T {
	sl.Lock()
	defer sl.Unlock()
	if sl.list.Len() == 0 {
		return nil
	}
	return sl.list.Front().Value.(T)
}

func (sl *SyncList[t]) ForEach(consumer func(value T) bool) {
	sl.Lock()
	defer sl.Unlock()

	for e := sl.list.Front(); e != nil; e = e.Next() {
		res := consumer(e.Value.(T))
		if !res {
			break
		}
	}
}

func (sl *SyncList[t]) Show() string {
	sl.Lock()
	defer sl.Unlock()

	showArr := make([]string, 0)
	sl.ForEach(func(value T) bool {
		showArr = append(showArr, fmt.Sprintf("%v", value))
		return true
	})

	return fmt.Sprintf("[%s]", strings.Join(showArr, " -> "))
}

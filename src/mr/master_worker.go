package mr

import (
	"6.5840/util"
	"fmt"
	"log"
	"os"
	"time"
)

type Status uint8

const (
	initializing Status = 1 << iota
	mapProcessing
	reduceProcessing
	completed

	checker = (1 << iota) - 1
)

type WorkerType uint8

const (
	mapWorker WorkerType = iota + 1
	reduceWorker
)

// MapFunc 输入文件名和文件内容 输出key-value键值对
type MapFunc func(string, string) []KeyValue

// ReduceFunc 从MapFunc的输出中找到一个key以及相同key的value 输出string结果
type ReduceFunc func(string, []string) string

const template string = "%s %s\n"

var (
	MapOutFilenameTemplate       = "map_out_%s"
	reduceResultFilenameTemplate = "reduce_out_%s"
	basePath                     = "/Users/zixuan4/Desktop/projects/codes/6.5840/src/"
	logger                       = log.New(os.Stdout, "LOG: ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
)

// Master 管理所有Worker 不实际工作
type Master struct {
	WorkerAmount int
	IdleWorker   *util.SyncList[WorkerInterface]
	MapFile      *util.SyncList[string]
	TaskAmount   int
	// 使用过的文件 不应该被其他worker使用
	ReduceTempFile *util.SyncList[string]
	CompletedFile  *util.SyncList[string]
	*util.ReentrantRWLock
	MapF    MapFunc
	ReduceF ReduceFunc
}

// MakeMaster 创建一个master节点实例 用于管理所有map节点和reduce节点.
//
// workerAmount: worker节点数量.
//
// filePaths: 文件路径名.
//
// mapF[mr.MapFunc]: map任务函数.
//
// reduceF: reduce任务函数.
func MakeMaster(workerAmount int, filePaths []string, mapF MapFunc, reduceF ReduceFunc) *Master {
	mapFileList := util.MakeSyncList[string]()
	for _, filename := range filePaths {
		mapFileList.Push(filename)
	}
	idleWorker := util.MakeSyncList[WorkerInterface]()

	m := &Master{
		WorkerAmount:    workerAmount,
		IdleWorker:      idleWorker,
		MapFile:         mapFileList,
		TaskAmount:      mapFileList.Len(),
		ReduceTempFile:  util.MakeSyncList[string](),
		CompletedFile:   util.MakeSyncList[string](),
		ReentrantRWLock: util.MakeReentrantLock(),
		MapF:            mapF,
		ReduceF:         reduceF,
	}
	m.initWorkers()
	return m
}

func (master *Master) makeMapWorker(filepath string, id int) *MapWorker {
	mw := &MapWorker{
		AbstractWorker: AbstractWorker{
			id:           id,
			Status:       initializing,
			WorkerType:   mapWorker,
			Master:       master,
			TaskFilePath: filepath,
		},
		MapF: master.MapF,
	}
	mw.WorkerInterface = mw
	return mw
}

func (master *Master) makeReduceWorker(id int) *ReduceWorker {
	rw := &ReduceWorker{
		AbstractWorker: AbstractWorker{
			id:         id,
			Status:     initializing,
			WorkerType: reduceWorker,
			Master:     master,
		},
		ReduceF: master.ReduceF,
	}
	rw.WorkerInterface = rw
	return rw
}

func (master *Master) initWorkers() {
	// 创建worker 并分配初始文件名 -> init状态
	// 之后worker 应该能从剩余未使用文件中读取文件并继续完成任务 -> processing状态时应该循环读取
	// reduce 任务需要循环读取中间文件名
	// 如果mapWorker没有map文件读取 则进入idle状态 对reduceWorker同理
	i, wa := 0, 0
	for value := master.MapFile.Poll(); value != nil; value = master.MapFile.Poll() {
		filePath := value.(string)
		if i >= master.WorkerAmount {
			// 任务编号[WorkerAmount, TaskAmount] 不能直接分配给worker的放回队列
			master.MapFile.Push(filePath)
		} else {
			// 任务编号[0, WorkerAmount)的任务 直接分配给worker
			mapWorker := master.makeMapWorker(filePath, i)
			master.IdleWorker.Push(mapWorker)
			wa++
		}
		if i >= master.TaskAmount {
			// 如果worker数量比任务数量还要多 则不会创建那么多数量的Worker
			break
		}
		i++
	}
	logger.Printf("创建map worker %d 个.", wa)

	reduceWorker := master.makeReduceWorker(1)
	logger.Println("创建reduce worker 1 个.")
	master.IdleWorker.Push(reduceWorker)
}

func (master *Master) Serve() {
	// 一个协程启动map任务
	mapChan := make(chan interface{}, 1)
	go master.scheduleWorker(mapChan, mapWorker)

	// 一个协程启动reduce任务
	reduceChan := make(chan interface{}, 1)
	go master.scheduleWorker(reduceChan, reduceWorker)

	// 主线程监听上面两个协程 都结束了之后该方法结束
	select {
	case _ = <-mapChan:
		break
	}
	fmt.Println("map task has finished.")
	select {
	case _ = <-reduceChan:
		break
	}
	fmt.Println("reduce task has finished.")
}

func (master *Master) restTask() int {
	master.Lock()
	defer master.Unlock()

	return master.TaskAmount - master.CompletedFile.Len()
}

func (master *Master) scheduleWorker(finishChan chan interface{}, t WorkerType) {
	for master.restTask() > 0 {
		worker := master.findIdleWorker(t)

		// 没有空闲的对象 则等待3s在进行调度
		if worker == nil {
			logger.Printf("[ID: %d] 没有找到空闲对象, 睡眠3s再进行调度\n", util.GoID())
			time.Sleep(time.Duration(500) * time.Millisecond)
			continue
		}

		go func() {
			logger.Printf("[id: %d] 找到空闲worker[%#v]\n", util.GoID(), worker)
			err := worker.start()
			if err != nil {
				logger.Printf("[id: %d] 在启动worker时失败, 详细原因: %s\n", util.GoID(), err)
			}
		}()
	}

	finishChan <- struct{}{}
}

// findIdleWorker 找到一个idle的worker 并将其置为busy
// 如果没有idle的worker 那么返回nil
// TODO: 目前锁粒度太大 遍历的效率差 可以改为两个队列
func (master *Master) findIdleWorker(t WorkerType) WorkerInterface {
	master.Lock()
	logger.Printf("[ID: %d] find idle type: %d, start lock.\n", util.GoID(), t)
	defer func() {
		logger.Printf("[ID: %d] find idle type: %d completed, unlock.\n", util.GoID(), t)
		master.Unlock()
	}()

	var (
		worker WorkerInterface
	)
	ok := false
	size := master.IdleWorker.Len()
	for !ok && size > 0 {
		w := master.IdleWorker.Poll()
		size--
		// 队列为空
		if w == nil {
			return nil
		}
		switch t {
		case reduceWorker:
			_, ok = w.(*ReduceWorker)
			break
		case mapWorker:
			_, ok = w.(*MapWorker)
			break
		}
		if ok {
			worker = w.(WorkerInterface)
			break
		}
		logger.Printf("[ID: %d] incorrect type worker: {%v}, find again.\n", util.GoID(), w)
		master.IdleWorker.Push(w)
	}

	return worker
}

func (master *Master) addIdleWorker(aw WorkerInterface) {
	master.IdleWorker.Push(aw)
}

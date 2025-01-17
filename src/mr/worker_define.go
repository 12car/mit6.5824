package mr

import (
	"fmt"
	"log"
	"maps"
	"os"
	"strings"
	"sync"
	"time"
)

type Status uint8

const (
	initializing Status = 1 << iota
	mapProcessing
	reduceProcessing
	completed
)

type WorkerType uint8

const (
	mapWorker WorkerType = iota
	reduceWorker
)

// Master 管理所有Worker 不实际工作
type Master struct {
	WorkerAmount int
	IdleWorker   []WorkerInterface
	BusyWorker   []WorkerInterface
	MapFileMap   map[string]bool
	// 使用过的文件 不应该被其他worker使用
	ReduceTempFileMap map[string]bool
	lock              sync.Locker
	MapF              MapFunc
	ReduceF           ReduceFunc
}

func MakeMaster(workerAmount int, filenames []string, mapF MapFunc, reduceF ReduceFunc) *Master {
	fileSet := make(map[string]bool)
	for _, filename := range filenames {
		fileSet[filename] = true
	}
	return &Master{
		WorkerAmount:      workerAmount,
		IdleWorker:        make([]WorkerInterface, 0),
		BusyWorker:        make([]WorkerInterface, 0),
		MapFileMap:        fileSet,
		ReduceTempFileMap: make(map[string]bool),
		lock:              new(sync.RWMutex),
		MapF:              mapF,
		ReduceF:           reduceF,
	}
}

func (master *Master) MakeMapWorker(filename string) *MapWorker {
	mw := &MapWorker{
		AbstractWorker: AbstractWorker{
			Status:       initializing,
			WorkerType:   mapWorker,
			Master:       master,
			TaskFileName: filename,
		},
		MapF: master.MapF,
	}
	mw.WorkerInterface = mw
	return mw
}

func (master *Master) MakeReduceWorker() *ReduceWorker {
	mw := &ReduceWorker{
		AbstractWorker: AbstractWorker{
			Status:     initializing,
			WorkerType: mapWorker,
			Master:     master,
		},
		ReduceF: master.ReduceF,
	}
	mw.WorkerInterface = mw
	return mw
}

func (master *Master) InitWorkers() {
	workerAmount := master.WorkerAmount
	fileNames := maps.Keys(master.MapFileMap)

	// 收集文件
	for fileName := range fileNames {
		master.MapFileMap[fileName] = true
	}

	// 创建worker 并分配初始文件名 -> init状态
	// 之后worker 应该能从剩余未使用文件中读取文件并继续完成任务 -> processing状态时应该循环读取
	// reduce 任务需要循环读取中间文件名
	// 如果mapWorker没有map文件读取 则进入idle状态 对reduceWorker同理
	i := 0
	for fileName := range master.MapFileMap {
		// 实际上的逻辑是 创建min(workerAmount, fileAmount)数量的worker
		if i >= workerAmount {
			break
		}
		i++
		mapWorker := master.MakeMapWorker(fileName)
		reduceWorker := master.MakeReduceWorker()
		master.IdleWorker = append(master.IdleWorker, mapWorker)
		master.IdleWorker = append(master.IdleWorker, reduceWorker)
		master.MapFileMap[fileName] = false
		log.Printf("创建map worker %d 个, 并将 %s 分配给新创建的worker.\n", i, fileName)
	}

	master.Serve()
}

func (master *Master) Serve() {
	// 一个协程启动map任务
	mapChan := make(chan interface{}, 1)
	go master.scheduleWorker(mapChan, reduceWorker)

	// 一个协程启动reduce任务
	reduceChan := make(chan interface{}, 1)
	go master.scheduleWorker(reduceChan, reduceWorker)

	// 一个协程监听上面两个协程 都结束了之后该方法结束
	go func(mapChan, reduceChan <-chan interface{}) {
		select {
		case _ = <-mapChan:
			break
		}
		select {
		case _ = <-reduceChan:
			break
		}
	}(mapChan, reduceChan)
}

func (master *Master) scheduleWorker(finishChan chan interface{}, t WorkerType) {
	master.lock.Lock()
	cantFinish := len(master.MapFileMap) != 0 && len(master.ReduceTempFileMap) != 0

	for cantFinish {
		var (
			worker WorkerInterface
			i      int
		)

		// 取 master.IdleWorker 中第一个 Worker
		for i, worker = range master.IdleWorker {
			var ok bool
			if t == reduceWorker {
				_, ok = worker.(*ReduceWorker)
			} else if t == mapWorker {
				_, ok = worker.(*MapWorker)
			}
			if ok {
				break
			}
		}

		// TODO: 这个方式效率不好 看看有没有更好的办法
		master.IdleWorker = append(master.IdleWorker[0:i], master.IdleWorker[i+1:]...)
		master.lock.Unlock()
		// 没有空闲的对象 则等待1s在进行调度
		if worker == nil {
			time.Sleep(1)
			continue
		}

		err := worker.doTask()

		if err != nil {
			// TODO: 该worker失败后 丢弃该worker 并且最后有能力在检测到不能完成任务时抛出异常通知coordinator结束程序
			continue
		}
		// 成功则放回IdleWorker
		master.lock.Lock()
		master.IdleWorker = append(master.IdleWorker, worker)
		master.lock.Unlock()
	}
	finishChan <- struct{}{}
}

func (master *Master) idleWorker(aw WorkerInterface) {
	// worker是多线程运行 需要防止多个worker同时结束操作队列
	master.lock.Lock()
	defer master.lock.Unlock()

	busyWorkers := master.BusyWorker
	for i, w := range busyWorkers {
		if aw == w {
			busyWorkers = append(busyWorkers[:i], busyWorkers[i+1:]...)
			break
		}
	}
	master.IdleWorker = append(master.IdleWorker, aw)

}

// WorkerInterface 具体worker实现接口
type WorkerInterface interface {
	// 做某个任务
	doTask() error
}

// AbstractWorker 抽象Worker
type AbstractWorker struct {
	Status
	WorkerType
	*Master
	WorkerInterface
	TaskFileName string
}

func (aw *AbstractWorker) finishTask() {
	aw.Status = completed
	aw.Master.idleWorker(aw.WorkerInterface)

	// 如果是完成了一个reduce任务 那么告诉coordinator有一个任务已完成
	if aw.WorkerType == reduceWorker {
		CallDone()
	}
}

func (aw *AbstractWorker) doTask() {
	if aw.WorkerType == mapWorker {
		aw.Status = mapProcessing
	} else {
		aw.Status = reduceProcessing
	}
	err := aw.WorkerInterface.doTask()
	if err != nil {
		log.Fatalf("")
	}
	aw.finishTask()
}

// MapFunc 输入文件名和文件内容 输出key-value键值对
type MapFunc func(string, string) []KeyValue

// ReduceFunc 从MapFunc的输出中找到一个key以及相同key的value 输出string结果
type ReduceFunc func(string, []string) string

type MapWorker struct {
	AbstractWorker
	MapF MapFunc
}

func (mw *MapWorker) doTask() error {
	log.Printf("[Map Worker] doTask function running.")
	filename := mw.TaskFileName
	lock := mw.Master.lock

	if filename == "" {
		lock.Lock()

	}

	// 执行map任务
	mw.MapF(filename, readFile(filename))

	lock.Lock()
	defer lock.Unlock()

	mapFileMap := mw.MapFileMap
	reduceFileMap := mw.ReduceTempFileMap
	delete(mapFileMap, filename)
	// map完一个之后存储reduce文件
	reduceFileMap[filename] = true

	return nil
}

type ReduceWorker struct {
	AbstractWorker
	ReduceF ReduceFunc
}

func (rw *ReduceWorker) doTask() error {
	log.Printf("[Reduce Worker] doTask function running.")
	lock := rw.Master.lock
	lock.Lock()

	var (
		m        map[string][]string
		filename string
	)
	// 找到一个可以用的temp file
	for filename, visited := range rw.ReduceTempFileMap {
		if visited {
			rw.ReduceTempFileMap[filename] = false
			lock.Unlock()
			filename = filename
			break
		}
	}
	// 读取临时文件mr_out_{filename}并执行reduce
	mapOutFilename := fmt.Sprintf(MapOutFilenameTemplate, filename)
	m = parseIntermediaFile(readFile(basePath + mapOutFilename))

	reduceResultFilename := fmt.Sprintf(reduceResultFilenameTemplate, filename)
	cFile, err := os.Create(basePath + reduceResultFilename)
	if err != nil {
		fmt.Printf("reduce result file creating has failed. caused by: %v\n", err)
	}

	buf := make([]byte, 0, 1024*64)
	for k, v := range m {
		result := rw.ReduceF(k, v)

		if cap(buf) < len(buf)+len(result) {
			fmt.Printf("write %d byte.\n", len(buf))
			canWrite := cap(buf) - len(buf)
			buf = append(buf, result[0:canWrite]...)
			result = result[canWrite:]
			_, _ = cFile.Write(buf)
			buf = buf[:0]
		}

		buf = append(buf, result...)
	}
	return nil
}

func readFile(filename string) string {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("read file: %s causing mistake for %v \n", filename, err)
	}
	return string(content)
}

// parseIntermediaFile 解析map产生的中间文件
func parseIntermediaFile(content string) map[string][]string {

	m := make(map[string][]string)
	kvLine := strings.Split(content, "\n")

	for _, kvRaw := range kvLine {
		kv := strings.Split(kvRaw, " ")

		if len(kv) != 2 {
			fmt.Printf("kv is error: [%s]\n", kvRaw)
			continue
		}

		values, exist := m[kv[0]]
		if !exist {
			values = make([]string, 0)
		}
		values = append(values, kv[1])
		m[kv[0]] = values
	}
	return m
}

const template string = "%s %s\n"

var (
	MapOutFilenameTemplate       string = "map_out_%s"
	reduceResultFilenameTemplate string = "reduce_out_%s"
	basePath                            = "/Users/zixuan4/Desktop/projects/codes/6.5840/src/"
)

// transferIntermediaFile 将keyValues以 '{key}-{value}\n' 的形式写入文件中
// return 文件名
func transferIntermediaFile(filename string, keyValues []KeyValue) string {

	buf := make([]byte, 0, 1024*64)
	intermediaFilename := fmt.Sprintf(MapOutFilenameTemplate, filename)
	intermediaFilePath := basePath + intermediaFilename
	cFile, err := os.Create(intermediaFilePath)
	defer func(cFile *os.File) {
		_ = cFile.Sync()
		_ = cFile.Close()
	}(cFile)

	if err != nil {
		log.Fatalf("create intermedia file: %s caused mistake: %v \n", intermediaFilename, err)
	}

	for _, kv := range keyValues {
		input := fmt.Sprintf(template, kv.Key, kv.Value)
		// 需要清空缓冲区
		if cap(buf) < len(input)+len(buf) {
			fmt.Printf("write %d byte.\n", len(buf))
			canWrite := cap(buf) - len(buf)
			buf = append(buf, input[0:canWrite]...)
			input = input[canWrite:]
			_, _ = cFile.Write(buf)
			buf = buf[:0]
		}
		buf = append(buf, input...)
	}
	return intermediaFilename
}

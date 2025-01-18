package mr

import (
	"6.5840/util"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
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

var (
	logger *log.Logger = log.New(os.Stdout, "LOG: ", log.Ldate|log.Ltime|log.Lshortfile)
)

// Master 管理所有Worker 不实际工作
type Master struct {
	WorkerAmount int
	IdleWorker   map[WorkerInterface]bool
	MapFileMap   map[string]bool
	// 使用过的文件 不应该被其他worker使用
	ReduceTempFileMap map[string]bool
	*util.ReentrantRWLock
	MapF    MapFunc
	ReduceF ReduceFunc
}

func MakeMaster(workerAmount int, filenames []string, mapF MapFunc, reduceF ReduceFunc) *Master {
	fileSet := make(map[string]bool)
	for _, filename := range filenames {
		fileSet[filename] = true
	}
	return &Master{
		WorkerAmount:      workerAmount,
		IdleWorker:        make(map[WorkerInterface]bool),
		MapFileMap:        fileSet,
		ReduceTempFileMap: make(map[string]bool),
		ReentrantRWLock:   util.MakeReentrantLock(),
		MapF:              mapF,
		ReduceF:           reduceF,
	}
}

func (master *Master) MakeMapWorker(filepath string, id int) *MapWorker {
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
	return mw
}

func (master *Master) MakeReduceWorker(id int) *ReduceWorker {
	mw := &ReduceWorker{
		AbstractWorker: AbstractWorker{
			id:         id,
			Status:     initializing,
			WorkerType: mapWorker,
			Master:     master,
		},
		ReduceF: master.ReduceF,
	}
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
		mapWorker := master.MakeMapWorker(fileName, i+1)
		i++
		master.IdleWorker[mapWorker] = true
		master.MapFileMap[fileName] = true
		logger.Printf("创建map worker %d 个, 并将 %s 分配给新创建的worker.\n", i, fileName)
	}
	reduceWorker := master.MakeReduceWorker(1)
	logger.Println("创建reduce worker 1 个.")
	master.IdleWorker[reduceWorker] = true

	master.Serve()
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

func (master *Master) scheduleWorker(finishChan chan interface{}, t WorkerType) {
	//logger.Printf("schedule type %d worker, with mapfile: {%v} and reducefile: {%v}\n", t, master.MapFileMap, master.ReduceTempFileMap)
	for len(master.MapFileMap) != 0 || len(master.ReduceTempFileMap) != 0 {
		//logger.Printf("schedule type %d worker, with mapfile: {%v} and reducefile: {%v}\n", t, master.MapFileMap, master.ReduceTempFileMap)
		worker := master.findIdleWorker(t)

		// 没有空闲的对象 则等待1s在进行调度
		if worker == nil {
			// logger.Printf("[id: %d] 没有找到空闲worker, 空闲队列: %v\n", util.GoID(), master.IdleWorker)
			time.Sleep(10)
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
func (master *Master) findIdleWorker(t WorkerType) WorkerInterface {
	master.Lock()
	defer master.Unlock()
	var (
		worker WorkerInterface
	)

	// 取 master.IdleWorker 中第一个 空闲的Worker
	for w, idle := range master.IdleWorker {
		if !idle {
			continue
		}
		var ok bool
		switch t {
		case reduceWorker:
			_, ok = w.(*ReduceWorker)
			break
		case mapWorker:
			_, ok = w.(*MapWorker)
			break
		}
		if ok {
			worker = w
			break
		}
	}

	_ = master.busyWorker(worker)
	return worker
}

func (master *Master) idleWorker(aw WorkerInterface) error {
	// worker是多线程运行 需要防止多个worker同时结束操作队列
	master.Lock()
	defer master.Unlock()

	available, exist := master.IdleWorker[aw]
	if !exist {
		available = true
	}

	if available {
		master.IdleWorker[aw] = true
		return nil
	} else {
		return errors.New(fmt.Sprintf("this worker: %#v is busy", aw))
	}
}

func (master *Master) busyWorker(aw WorkerInterface) error {
	// worker是多线程运行 需要防止多个worker同时结束操作队列
	master.Lock()
	defer master.Unlock()

	available, exist := master.IdleWorker[aw]
	if !exist {
		available = true
	}

	if available {
		master.IdleWorker[aw] = false
		return nil
	} else {
		return errors.New(fmt.Sprintf("this worker: %#v is busy", aw))
	}
}

// WorkerInterface 具体worker实现接口
type WorkerInterface interface {
	// 开始作业
	start() error
	// doTask 做某个任务
	doTask() error
	// finishTask 切换完成状态、清理任务队列、切换空闲队列、通知
	finishTask(error)
}

// AbstractWorker 抽象Worker
type AbstractWorker struct {
	id int
	Status
	WorkerType
	Master *Master
	WorkerInterface
	TaskFilePath string
}

func (aw *AbstractWorker) start() error {
	logger.Printf("[worker start] worker-%d 开始工作\n", aw.id)

	if aw.Status&checker == 0 || aw.WorkerType == 0 || aw.Master == nil {
		return errors.New(fmt.Sprintf("当前worker参数不完全, "+
			"Status is mistake: {%v}, WorkerType is mistake: {%v}, Master is nil: {%v}\n",
			aw.Status&checker == 0, aw.WorkerType == 0, aw.Master == nil))
	}

	// 已经有其他线程修改了状态
	if aw.Status&(initializing|completed) == 0 {
		return errors.New("当前worker非空闲状态, 不能执行任务")
	}

	err := aw.Master.busyWorker(aw)

	if err != nil {
		logger.Printf("worker进入工作队列失败, worker: {%#v}, err: {%v}\n", aw, err)
		return err
	}

	if aw.WorkerType == mapWorker {
		aw.Status = mapProcessing
	} else {
		aw.Status = reduceProcessing
	}

	logger.Printf("[ID: %d] worker-{%d}开始工作, type: {%d}, 目标文件路径: {%s}\n",
		util.GoID(), aw.id, aw.WorkerType, aw.TaskFilePath)
	err = aw.doTask()
	logger.Printf("[ID: %d] worker-{%d}工作完成, type: {%d}, 开始结束任务工作\n", util.GoID(), aw.id, aw.WorkerType)
	aw.finishTask(err)

	if err != nil {
		return err
	}

	return nil
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
	logger.Printf("[Map Worker] doTask function running.")
	lock := mw.Master.Locker

	// 从文件队列中找到一个还没有执行的文件
	if mw.TaskFilePath == "" {
		lock.Lock()
		for availableFilename, visited := range mw.Master.MapFileMap {
			if visited {
				mw.Master.MapFileMap[availableFilename] = false
				mw.TaskFilePath = availableFilename
			}
		}
		lock.Unlock()
	}

	if mw.TaskFilePath == "" {
		logger.Println("无Map任务执行")
		return nil
	}

	taskFilePath := mw.TaskFilePath
	_, filename := path.Split(taskFilePath)
	logger.Printf("[Map Worker] target filename: %s\n", taskFilePath)
	defer func() {
		if r := recover(); r != nil {
			mw.Master.MapFileMap[taskFilePath] = true
			logger.Printf("执行map任务时发生异常: %#v, 将文件{%s}放回队列.\n", r, taskFilePath)
		}
	}()

	// 执行map任务
	KeyValues := mw.MapF(filename, readFile(taskFilePath))
	intermediaFilename := transferIntermediaFile(filename, KeyValues)
	logger.Printf("map worker {worker-%d} 执行map任务完成, 开始写入中间文件 {%s} \n", mw.id, intermediaFilename)

	return nil
}

func (mw *MapWorker) finishTask(preErr error) {
	logger.Printf("map worker %d finish task file %s. ", mw.id, mw.TaskFilePath)
	if preErr != nil {
		logger.Printf("preErr: %s\n", preErr)
	} else {
		logger.Printf("\n")
	}
	mw.Status = completed
	_ = mw.Master.idleWorker(mw)

	master := mw.Master
	master.Lock()
	defer master.Unlock()

	delete(mw.Master.MapFileMap, mw.TaskFilePath)
	// 上一步没有出问题 则map完一个之后存储reduce文件
	if preErr == nil {
		_, taskFilename := filepath.Split(mw.TaskFilePath)
		intermediaFilePath := basePath + fmt.Sprintf(MapOutFilenameTemplate, taskFilename)
		mw.Master.ReduceTempFileMap[intermediaFilePath] = true
	}
	mw.TaskFilePath = ""
}

type ReduceWorker struct {
	AbstractWorker
	ReduceF ReduceFunc
}

func (rw *ReduceWorker) doTask() error {
	logger.Printf("[Reduce Worker] doTask function running.")
	lock := rw.Master.Locker

	var (
		m        map[string][]string
		filename string
	)
	// 找到一个可以用的temp file
	lock.Lock()
	for availableFilename, visited := range rw.Master.ReduceTempFileMap {
		if visited {
			rw.Master.ReduceTempFileMap[availableFilename] = false
			filename = availableFilename
			break
		}
	}
	lock.Unlock()

	// 没有找到可用的temp file
	if filename == "" {
		logger.Println("无Reduce任务执行")
		return nil
	}
	rw.TaskFilePath = filename
	// 读取临时文件mr_out_{filename}并执行reduce
	logger.Printf("reduce worker {worker-%d} 开始读取中间文件 {%s} \n", rw.id, rw.TaskFilePath)
	m = parseIntermediaFile(readFile(basePath + rw.TaskFilePath))

	reduceResultFilename := fmt.Sprintf(reduceResultFilenameTemplate, filename)
	cFile, err := os.Create(basePath + reduceResultFilename)
	if err != nil {
		fmt.Printf("reduce result file creating has failed. caused by: %v\n", err)
	}

	logger.Printf("reduce worker {worker-%d} 开始写入结果文件 {%s} \n", rw.id, basePath+reduceResultFilename)
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

func (rw *ReduceWorker) finishTask(preErr error) {
	if preErr != nil {
		logger.Printf("preErr: %v\n", preErr)
	} else {
		logger.Printf("\n")
	}
	rw.Status = completed
	err := rw.Master.idleWorker(rw)

	if err != nil {
		logger.Fatalf("idle worker fail: %v.\n", err)
	}

	delete(rw.Master.ReduceTempFileMap, rw.TaskFilePath)

	// 如果是完成了一个reduce任务 那么告诉coordinator有一个任务已完成
	if preErr == nil {
		rw.Master.Lock()
		defer rw.Master.Unlock()
		// TODO: 打开
		//CallDone()
	}
	rw.TaskFilePath = ""
}

func readFile(filePath string) string {
	content, err := os.ReadFile(filePath)
	if err != nil {
		logger.Fatalf("read file: %s causing mistake for %v \n", filePath, err)
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
		logger.Fatalf("create intermedia file: %s caused mistake: %v \n", intermediaFilename, err)
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

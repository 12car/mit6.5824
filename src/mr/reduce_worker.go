package mr

import (
	"6.5840/util"
	"fmt"
	"os"
	"path/filepath"
)

type ReduceWorker struct {
	AbstractWorker
	ReduceF ReduceFunc
}

func (rw *ReduceWorker) doTask() error {
	logger.Printf("[ID: %d] [Reduce Worker] doTask function running.\n", util.GoID())

	var (
		m        map[string][]string
		filePath string
	)
	// 找到一个可以用的temp file
	if rw.Master.ReduceTempFile.Len() == 0 {
		logger.Printf("[ID: %d] 无Reduce任务执行", util.GoID())
		return nil
	}

	e := rw.Master.ReduceTempFile.Poll()
	// 无锁状态 可能为nil
	if e == nil {
		logger.Printf("[ID: %d] 无Reduce任务执行", util.GoID())
		return nil
	}

	filePath, ok := e.(string)

	if ok {
		rw.TaskFilePath = filePath
	}

	defer func() {
		if r := recover(); r != nil && rw.TaskFilePath != "" {
			rw.Master.ReduceTempFile.Push(rw.TaskFilePath)
		}
	}()
	// 读取临时文件mr_out_{filePath}并执行reduce
	logger.Printf("[ID: %d] reduce worker {worker-%d} 开始读取中间文件 {%s} \n", util.GoID(), rw.id, rw.TaskFilePath)
	m = parseIntermediaFile(readFile(rw.TaskFilePath))

	// 创建reduce结果
	_, filename := filepath.Split(filePath)
	reduceResultFilename := fmt.Sprintf(reduceResultFilenameTemplate, filename)
	cFile, err := os.Create(basePath + reduceResultFilename)
	defer func() {
		_ = cFile.Sync()
		_ = cFile.Close()
	}()

	if err != nil {
		fmt.Printf("[ID: %d] reduce result file creating has failed. caused by: %v\n", util.GoID(), err)
	}

	logger.Printf("[ID: %d] reduce worker {worker-%d} 开始写入结果文件 {%s} \n", util.GoID(), rw.id, basePath+reduceResultFilename)
	buf := make([]byte, 0, 1024*64)
	for k, v := range m {
		result := rw.ReduceF(k, v)
		input := fmt.Sprintf("%s %s\n", k, result)

		if cap(buf) <= len(buf)+len(input) {
			fmt.Printf("write %d byte.\n", len(buf))
			canWrite := cap(buf) - len(buf)
			buf = append(buf, input[0:canWrite]...)
			input = input[canWrite:]
			_, _ = cFile.Write(buf)
			buf = buf[:0]
		}

		buf = append(buf, input...)
	}

	if len(buf) > 0 {
		fmt.Printf("write %d byte.\n", len(buf))
		_, _ = cFile.Write(buf)
	}
	return nil
}

func (rw *ReduceWorker) finishTask(preErr error) {
	if preErr != nil {
		logger.Printf("preErr: %v\n", preErr)
	}
	rw.Status = completed
	rw.Master.addIdleWorker(rw)

	// 如果是完成了一个reduce任务 那么告诉coordinator有一个任务已完成
	if preErr == nil && rw.TaskFilePath != "" {
		rw.Master.CompletedFile.Push(rw.TaskFilePath)
	}
	rw.TaskFilePath = ""
}

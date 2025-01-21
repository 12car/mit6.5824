package mr

import (
	"6.5840/util"
	"fmt"
	"path"
	"path/filepath"
)

type MapWorker struct {
	AbstractWorker
	MapF MapFunc
}

func (mw *MapWorker) doTask() error {
	logger.Printf("[Map Worker] doTask function running.")

	// 从文件队列中找到一个还没有执行的文件
	if mw.TaskFilePath == "" {
		e := mw.Master.MapFile.Poll()
		if e == nil {
			logger.Printf("[ID: %d] 无Map任务执行", util.GoID())
			return nil
		}
		filePath, ok := e.(string)
		if ok {
			mw.TaskFilePath = filePath
		}
	}

	if mw.TaskFilePath == "" {
		logger.Printf("[ID: %d] 无Map任务执行", util.GoID())
		return nil
	}

	taskFilePath := mw.TaskFilePath
	_, filename := path.Split(taskFilePath)
	logger.Printf("[Map Worker] target filename: %s\n", taskFilePath)
	defer func() {
		if r := recover(); r != nil {
			mw.Master.MapFile.Push(taskFilePath)
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
	logger.Printf("[ID: %d] map worker %d finish task file %s. ", util.GoID(), mw.id, mw.TaskFilePath)
	if preErr != nil {
		logger.Printf("preErr: %s\n", preErr)
	}
	mw.Status = completed

	if preErr == nil && mw.TaskFilePath != "" {
		_, taskFilename := filepath.Split(mw.TaskFilePath)
		intermediaFilePath := basePath + fmt.Sprintf(MapOutFilenameTemplate, taskFilename)
		mw.Master.ReduceTempFile.Push(intermediaFilePath)
	}
	// 上一步没有出问题 则map完一个之后存储reduce文件
	mw.TaskFilePath = ""
	mw.Master.addIdleWorker(mw)
}

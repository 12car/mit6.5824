package mr

import (
	"6.5840/util"
	"errors"
	"fmt"
)

// WorkerInterface 具体worker实现接口
type WorkerInterface interface {
	// start 开始作业
	// 1. idle队列出队(保证原子, 只有一个线程可以拿到某id的idle worker)
	// 2. 分配任务上下文()
	// 3. initializing -> mapProcessing or -> reduceProcessing
	start() error
	// doTask 执行map/reduce任务
	doTask() error
	// finishTask 切换完成状态、清理任务队列、将自己加入idle队列、通知
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
	logger.Printf("[ID: %d] [worker start] worker-%d 开始工作\n", util.GoID(), aw.id)

	if aw.Status&checker == 0 || aw.WorkerType == 0 || aw.Master == nil {
		return errors.New(fmt.Sprintf("当前worker参数不完全, "+
			"Status is mistake: {%v}, WorkerType is mistake: {%v}, Master is nil: {%v}\n",
			aw.Status&checker == 0, aw.WorkerType == 0, aw.Master == nil))
	}

	// 已经有其他线程修改了状态
	if aw.Status&(initializing|completed) == 0 {
		return errors.New("当前worker非空闲状态, 不能执行任务")
	}

	if aw.WorkerType == mapWorker {
		aw.Status = mapProcessing
	} else if aw.WorkerType == reduceWorker {
		aw.Status = reduceProcessing
	}

	logger.Printf("[ID: %d] worker-{%d}开始工作, type: {%d}, 目标文件路径: {%s}\n",
		util.GoID(), aw.id, aw.WorkerType, aw.TaskFilePath)
	err := aw.doTask()
	logger.Printf("[ID: %d] worker-{%d}工作完成, type: {%d}, 开始结束任务工作\n", util.GoID(), aw.id, aw.WorkerType)
	aw.finishTask(err)

	if err != nil {
		return err
	}

	return nil
}

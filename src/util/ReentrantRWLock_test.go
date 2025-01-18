package util

import (
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var Lock *ReentrantRWLock = MakeReentrantLock()

var filePaths []string = []string{
	"../main/pg-being_ernest.txt",
	"../main/pg-dorian_gray.txt",
	"../main/pg-frankenstein.txt",
	"../main/pg-grimm.txt",
	"../main/pg-huckleberry_finn.txt",
	"../main/pg-metamorphosis.txt",
	"../main/pg-sherlock_holmes.txt",
	"../main/pg-tom_sawyer.txt",
}

func getFilePathMap() map[string]bool {
	m := make(map[string]bool)
	for _, filePath := range filePaths {
		m[filePath] = true
	}
	return m
}

var FilePathMap map[string]bool = getFilePathMap()

func TestReentrantRWLock_Lock(t *testing.T) {
	wg := new(sync.WaitGroup)

	wg.Add(3)
	go func() {
		for len(FilePathMap) > 0 {
			filePath := findAvailable()

			if filePath == "" {
				n := rand.Int63n(7) + 3
				log.Printf("[ID: %d] 没有找到可用文件, 睡眠%d秒\n", GoID(), n)
				time.Sleep(time.Duration(n) * time.Second)
				continue
			}

			Lock.Lock()
			log.Printf("[ID: %d] 删除key: %s\n", GoID(), filePath)
			delete(FilePathMap, filePath)
			Lock.Unlock()

			n := rand.Int63n(7) + 3
			log.Printf("[ID: %d] 删除key成功, 睡眠%d秒\n", GoID(), n)
			time.Sleep(time.Duration(n) * time.Second)
		}

		wg.Done()
	}()

	go func() {
		for len(FilePathMap) > 0 {
			filePath := findAvailable()

			if filePath == "" {
				n := rand.Int63n(7) + 3
				log.Printf("[ID: %d] 没有找到可用文件, 睡眠%d秒\n", GoID(), n)
				time.Sleep(time.Duration(n) * time.Second)
				continue
			}

			Lock.Lock()
			log.Printf("[ID: %d] 删除key: %s\n", GoID(), filePath)
			delete(FilePathMap, filePath)
			Lock.Unlock()

			n := rand.Int63n(7) + 3
			log.Printf("[ID: %d] 删除key成功, 睡眠%d秒\n", GoID(), n)
			time.Sleep(time.Duration(n) * time.Second)
		}
		wg.Done()
	}()

	go func() {
		for len(FilePathMap) > 0 {
			Lock.Lock()
			l := rand.Intn(len(FilePathMap))
			if rand.Int()%2 == 0 {
				if l != 0 {
					FilePathMap[filePaths[l]] = true
					log.Printf("[ID: %d] 添加一个filepath{%s}, 且false\n", GoID(), filePaths[l])
				}
			}
			Lock.Unlock()
			n := rand.Int63n(10) + 5
			log.Printf("[ID: %d] 成功捣蛋! 睡眠%d秒\n", GoID(), n)
			time.Sleep(time.Duration(n) * time.Second)
		}

		wg.Done()
	}()

	wg.Wait()
}

func findAvailable() string {
	Lock.Lock()
	defer Lock.Unlock()

	for filePath, available := range FilePathMap {
		if available {
			return filePath
		}
	}

	return ""
}

package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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

// transferIntermediaFile 将keyValues以 '{key}-{value}\n' 的形式写入文件中
// return 文件名
func transferIntermediaFile(filename string, keyValues []KeyValue) string {

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

	buf := make([]byte, 0, 1024*64)
	for _, kv := range keyValues {
		input := fmt.Sprintf(template, kv.Key, kv.Value)
		// 需要清空缓冲区
		if cap(buf) <= len(input)+len(buf) {
			fmt.Printf("write %d byte.\n", len(buf))
			canWrite := cap(buf) - len(buf)
			buf = append(buf, input[:canWrite]...)
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
	return intermediaFilename
}

func readFile(filePath string) string {
	content, err := os.ReadFile(filepath.Clean(filePath))
	if err != nil {
		logger.Fatalf("read file: %s causing mistake for %v \n", filePath, err)
	}
	return string(content)
}

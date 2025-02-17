package utils

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"
)

func ReadFile(path string) (*bytes.Buffer, error) {
	fmt.Println("Reading file", path)
	time.Sleep(1 * time.Second)
	fileHandle, err := os.Open(path)
	if err != nil {
		fmt.Printf("unable to open file %s: %s\n", path, err.Error())
		return new(bytes.Buffer), fmt.Errorf("unable to open file %s: %w", path, err)
	}
	defer fileHandle.Close()
	b := new(bytes.Buffer)
	_, err = io.Copy(b, fileHandle)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("unable to read file %s: %w", path, err)
	}
	return b, nil
}

package utils

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func ReadFile(path string) (*bytes.Buffer, error) {
	fmt.Println("Reading file", path)
	fileHandle, err := os.Open(path)
	if err != nil {
		fmt.Printf("Error opening file %s: %s\n", path, err)
		return new(bytes.Buffer), fmt.Errorf("unable to open file %s: %w", path, err)
	}
	// defer fileHandle.Close()

	b := new(bytes.Buffer)
	_, err = io.Copy(b, fileHandle)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("unable to read file %s: %w", path, err)
	}
	fmt.Println("File read successfully")
	return b, nil
}

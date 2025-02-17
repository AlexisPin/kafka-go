package utils

import (
	"bytes"
	"fmt"
	"os"
)

func ReadFile(path string) (*bytes.Buffer, error) {
	fmt.Printf("Reading file %s\n", path)
	buffer, err := os.ReadFile(path)
	fmt.Printf("Read file %s\n", path)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("unable to read file %s: %w", path, err)
	}
	b := bytes.NewBuffer(buffer)
	return b, nil
}

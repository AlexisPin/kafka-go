package utils

import (
	"bytes"
	"fmt"
	"os"
)

func ReadFile(path string) (*bytes.Buffer, error) {
	buffer, err := os.ReadFile(path)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("unable to read file %s: %w", path, err)
	}
	b := bytes.NewBuffer(buffer)
	return b, nil
}

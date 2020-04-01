package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// absolute path by relative path
func AbsPath() string, error {
	return filepath.Abs(filepath.Dir(os.Args[0]))
}
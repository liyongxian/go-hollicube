package utils

import (
	"log"
	"os"
	"path/filepath"
)

func AbsPath() string {
	// absolute path by relative path
	absPath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalln(err)
	}
	return absPath
}
package main

import (
	"os"
)

func directoryExists(filePath string) bool {
	info, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

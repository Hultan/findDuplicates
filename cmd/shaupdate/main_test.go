package main

import (
	"io/fs"
	"path/filepath"
	"regexp"
)

func scanPath(dir, pattern string) ([]string, error) {
	result := []string{}

	err := filepath.WalkDir(
		dir, func(s string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				ok, err := regexp.Match(pattern, []byte(s))
				if err != nil {
					return err
				}
				if ok {
					result = append(result, s)
				}
			}
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return result, nil
}

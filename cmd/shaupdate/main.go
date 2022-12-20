package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"

	_ "github.com/mattn/go-sqlite3"
)

const (
	testDbPath = "/home/per/code/shaupdate/test/data/test.db"
)

type File struct {
	Path         string
	HashComplete string
	HashPartial  string
}

const dbPath = "/home/per/files.db"

// USAGE
//
//	shaupdate -d
//		creates a database (files.db) in the current directory
//	shaupdate [DBPATH] [SCANPATH] [-c|--check]
func main() {
	// Create a files.db database
	createDatabaseMode()

	// Scan directory and create partial hashes for new files
	partialHashMode()

	// List all duplicate hashes (based on partial hash)
	listDuplicatesMode()

	// Delete database, we start with a new database every time
	deleteDatabase()
}

func deleteDatabase() {
	err := os.Remove(dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to remove database at %s...", dbPath)
		os.Exit(1)
	}
}

func listDuplicatesMode() {
	db := openDatabase(dbPath)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	dup, err := findDuplicatePartialHashes(db)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to find duplicates! Reason = %s", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("============")
	fmt.Println(" Duplicates ")
	fmt.Println("============")
	fmt.Println()
	fmt.Printf("%d duplicates found...\n", len(dup))
	fmt.Println()
	for _, file := range dup {
		fmt.Printf("%s\t%s\n", file.HashPartial, file.Path)
	}
}

func partialHashMode() {
	scanPath := verifyScanPathExists()

	db := openDatabase(dbPath)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	fileNames, err := scanDirectoryForFileNames(scanPath, "(.avi|.AVI|.mkv|.mp4|.MP4|.mpg|.wmv)$")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "scanDirectoryForFileNames() failed! Reason = %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("found %d new files...\n", len(fileNames))

	files := generateFilesFromFileNames(fileNames)
	for _, file := range files {
		fmt.Printf("generating hash for %s...\n", file.Path)
		err = insertFileWithHash(db, file.Path, file.HashPartial, file.HashComplete)
		if err != nil {
			// Failed to insert file, log and continue
			_, _ = fmt.Fprintf(os.Stderr, "failed to insert file inte database! Reason = %s\n", err)
			continue
		}
	}
}

func generateFilesFromFileNames(fileNames []string) []*File {
	var files []*File

	for _, fileName := range fileNames {
		file := &File{Path: fileName}

		hash, err := calculateHash(file.Path)
		if err != nil {
			panic(err)
		}
		file.HashPartial = hash

		files = append(files, file)
	}
	return files
}

func verifyScanPathExists() string {
	scanPath := "/media/x"
	if !verifyDirectoryExists(scanPath) {
		_, _ = fmt.Fprintf(os.Stderr, "scan path does not exist, don't forget to mount /media/x...\n")
		os.Exit(1)
	}

	return scanPath
}

func createDatabaseMode() {
	fmt.Printf("Creating database at : %s\n", dbPath)

	db, err := createDatabase(dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create database! Reason = %s\n", err)
		os.Exit(1)
	}

	_ = db.Close()
}

func calculateHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	r := bufio.NewReader(f)
	b := make([]byte, 65536)
	n, err := r.Read(b)
	if err == io.EOF {
		return "", errors.New("file is empty")
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b[0:n])), nil
}

func scanDirectoryForFileNames(dir, pattern string) ([]string, error) {
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

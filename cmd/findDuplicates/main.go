package main

import (
	"bufio"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	testDbPath = "/home/per/code/findDuplicates/test/data/test.db"
)

type File struct {
	Path         string
	HashComplete string
	HashPartial  string
}

const dbPath = "/home/per/files.db"

var scanPath = ""

func main() {
	if len(os.Args) != 2 {
		fmt.Println("USAGE : findDuplicates [ScanPath]")
		fmt.Println("See Obsidian for how to mount drives...")
		os.Exit(0)
	}

	scanPath = os.Args[1]
	if !directoryExists(scanPath) {
		msg := "scan path does not exist, have you forgotten to mount the drive? See Obsidian...\n"
		_, _ = fmt.Fprintf(os.Stderr, msg)
		os.Exit(1)
	}

	// Create a files.db database
	createDatabaseStep()

	// Scan directory and create partial hashes for new files
	partialHashStep()

	// List all duplicate hashes (based on partial hash)
	listDuplicatesStep()

	// Delete database, we start with a new database every time
	deleteDatabaseStep()
}

func createDatabaseStep() {
	fmt.Printf("Creating database at : %s\n\n", dbPath)

	db, err := createDatabase(dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create database! Reason = %s\n", err)
		os.Exit(1)
	}

	_ = db.Close()
}
func partialHashStep() {
	db := openDatabase(dbPath)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	start := time.Now()
	fmt.Println("Scanning directory...")
	fileNames, err := scanDirectory(scanPath, "(.avi|.AVI|.mkv|.mp4|.MP4|.mpg|.wmv)$")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "scanDirectory() failed! Reason = %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Scan result : found %d files (%s).\n\n", len(fileNames), time.Since(start).String())

	start = time.Now()
	fmt.Println("Generating hashes...")
	files, count := generatingHashes(fileNames)
	fmt.Printf("Generate result : generated hashes for %d files (%s).\n\n", count, time.Since(start).String())

	start = time.Now()
	fmt.Println("Inserting hashes into db...")
	count = 0
	for _, file := range files {
		// fmt.Printf("generating hash for %s...\n", file.Path)
		err = insertFileWithHash(db, file.Path, file.HashPartial, file.HashComplete)
		if err != nil {
			// Failed to insert file, log and continue
			_, _ = fmt.Fprintf(os.Stderr, "failed to insert file inte database! Reason = %s\n", err)
			continue
		}
		count++
	}
	fmt.Printf("Insert result : inserted hashes for %d files (%s).\n", count, time.Since(start).String())
}

func listDuplicatesStep() {
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

func deleteDatabaseStep() {
	err := os.Remove(dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to remove database at %s...", dbPath)
		os.Exit(1)
	}
}

func scanDirectory(dir, pattern string) ([]string, error) {
	// Needs to be declared like this (and not nil slice) for test to work
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

func generatingHashes(fileNames []string) ([]*File, int) {
	var files []*File
	gen := 0

	// For each file, calculate hash and generate a File{} struct
	for _, fileName := range fileNames {
		file := &File{Path: fileName}

		hash, err := calculateHash(file.Path)
		if err != nil {
			panic(err)
		}
		file.HashPartial = hash

		files = append(files, file)
		gen++
	}
	return files, gen
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

	// Read the first 64k of the file
	r := bufio.NewReader(f)
	b := make([]byte, 65536)
	n, err := r.Read(b)
	if err == io.EOF {
		return "", errors.New("file is empty")
	}
	if err != nil {
		return "", err
	}

	// Return MD5 hash of the file
	m := md5.New()
	return fmt.Sprintf("%x", m.Sum(b[0:n])), nil
}

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
	"path"
	"path/filepath"
	"regexp"

	_ "github.com/mattn/go-sqlite3"
)

type HashType int

const (
	PartialHash HashType = iota
	CompleteHash
)

const (
	testDbPath = "/home/per/code/shaupdate/test/data/test.db"
)

type File struct {
	Path         string
	HashComplete string
	HashPartial  string
}

// USAGE
//
//	shaupdate -d
//		creates a database (files.db) in the current directory
//	shaupdate [DBPATH] [SCANPATH] [-c|--check]
func main() {
	if len(os.Args) <= 1 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "-d":
		// Create a files.db database
		createDatabaseMode()
	case "-p":
		// Scan directory and create partial hashes for new files
		partialHashMode()
	case "-c":
		// Create complete hashes for duplicates
		completeHashMode()
	case "-lp":
		// List all duplicate hashes (based on partial hash)
		listDuplicatesMode(PartialHash)
	case "-lc":
		// List all duplicate hashes (based on complete hash)
		listDuplicatesMode(CompleteHash)
	default:
		printUsage()
		os.Exit(1)
	}

	os.Exit(0)
}

func listDuplicatesMode(hash HashType) {
	// findDuplicates(db)
}

func completeHashMode() {
	// sort.Slice(
	// 	files, func(i, j int) bool {
	// 		return files[i].HashPartial < files[j].HashPartial
	// 	},
	// )
	//
	// var prev *File
	// for _, file := range files {
	// 	if prev == nil {
	// 		prev = file
	// 		continue
	// 	}
	//
	// 	if prev.HashPartial == file.HashPartial {
	// 		handleIdenticalFiles(prev, file)
	// 	}
	//
	// 	prev = file
	// }

}

func partialHashMode() {
	dbPath := verifyDatabaseExists()
	scanPath := verifyScanPathExists()

	db := openDatabase(dbPath)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	fileNames, err := scanDirectoryForFileNames(scanPath, ".go$")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "scanDirectoryForFileNames() failed! Reason = %s", err)
		os.Exit(1)
	}

	fileNames = removeExistingFiles(db, fileNames)

	files := generateFilesFromFileNames(fileNames)
	for _, file := range files {
		err = insertFileWithHash(db, file.Path, file.HashPartial, file.HashComplete)
		if err != nil {
			// Failed to insert file, log and continue
			_, _ = fmt.Fprintf(os.Stderr, "failed to insert file inte database! Reason = %s", err)
			continue
		}
	}
}

func removeExistingFiles(db *sql.DB, fileNames []string) []string {
	var result []string

	for _, fileName := range fileNames {
		ok, err := existsFile(db, fileName)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to check if file exists! Reason = %s", err)
			continue
		}
		if !ok {
			result = append(result, fileName)
		}
	}

	return result
}

func generateFilesFromFileNames(fileNames []string) []*File {
	var files []*File

	for _, fileName := range fileNames {
		file := &File{Path: fileName}

		hash, err := calculateHash(file.Path, PartialHash)
		if err != nil {
			panic(err)
		}
		file.HashPartial = hash

		files = append(files, file)
	}
	return files
}

func verifyScanPathExists() string {
	if len(os.Args) <= 2 {
		printUsage()
		os.Exit(1)
	}

	scanPath, err := filepath.Abs(os.Args[2])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid scan path")
		os.Exit(1)
	}
	if !verifyDirectoryExists(scanPath) {
		_, _ = fmt.Fprintf(os.Stderr, "scan path does not exist")
		os.Exit(1)
	}

	return scanPath
}

func verifyDatabaseExists() string {
	dbPath, err := getDatabasePath()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to get database path! Reason = %s\n", err)
		os.Exit(1)
	}

	if !verifyFileExists(dbPath) {
		_, _ = fmt.Fprintf(os.Stderr, "database is missing in the current directory!\n")
		os.Exit(1)
	}
	return dbPath
}

func createDatabaseMode() {
	dbPath := verifyDatabaseExists()

	_, err := createDatabase(dbPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create database! Reason = %s\n", err)
		os.Exit(1)
	}
}

func getDatabasePath() (string, error) {
	dbPath, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}

	dbPath = path.Join(dbPath, databaseName)
	return dbPath, err
}

func printUsage() {
	_, _ = fmt.Fprintf(os.Stderr, "\nUSAGE:\n\n")
	_, _ = fmt.Fprintf(os.Stderr, "shaupdate -d\n")
	_, _ = fmt.Fprintf(os.Stderr, "        creates a files.db in the current directory\n")
	_, _ = fmt.Fprintf(os.Stderr, "shaupdate -p [SCANPATH]\n")
	_, _ = fmt.Fprintf(os.Stderr, "        creates partial hashes for all files in SCANPATH\n")
	_, _ = fmt.Fprintf(os.Stderr, "shaupdate -c\n")
	_, _ = fmt.Fprintf(os.Stderr, "        creates complete hashes for all duplicate files in the database\n")
	_, _ = fmt.Fprintf(os.Stderr, "shaupdate -lp\n")
	_, _ = fmt.Fprintf(os.Stderr, "        list all duplicates in database (partial hash)\n")
	_, _ = fmt.Fprintf(os.Stderr, "shaupdate -lc\n")
	_, _ = fmt.Fprintf(os.Stderr, "        list all duplicates in database (complete hash)\n")
}

func handleIdenticalFiles(prev *File, file *File) {
	assertCompleteHash(prev)
	assertCompleteHash(file)
	if prev.HashComplete == file.HashComplete {
		fmt.Println("Found identical files:")
		fmt.Printf("%s\t%s\n", prev.HashComplete, prev.Path)
		fmt.Printf("%s\t%s\n", file.HashComplete, file.Path)
		fmt.Println()
	}
}

func assertCompleteHash(file *File) {
	if file.HashComplete == "" {
		hash, err := calculateHash(file.Path, CompleteHash)
		if err != nil {
			panic(err)
		}
		file.HashComplete = hash
	}
}

func calculateHash(filePath string, hashType HashType) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if hashType == CompleteHash {
		hash := sha256.New()
		if _, err = io.Copy(hash, f); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	} else {
		r := bufio.NewReader(f)
		b := make([]byte, 2048)
		n, err := r.Read(b)
		if err == io.EOF {
			return "", errors.New("file is empty")
		}
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", sha256.Sum256(b[0:n])), nil
	}
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

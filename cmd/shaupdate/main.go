package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

type HashType int

const (
	PartialHash HashType = iota
	CompleteHash
)

var baseDir = "/home/per/temp/shrink/"
var testDbFolder = "/home/per/code/shaupdate/test/walk"
var testDbPath = "/home/per/code/shaupdate/test/data/test.db"
var existsDbPath = "/home/per/code/shaupdate/test/data/exists.db"
var notExistsDbPath = "/home/per/code/shaupdate/test/data/notExists.db"

type File struct {
	Path         string
	HashComplete string
	HashPartial  string
}

func main() {
	var db *sql.DB
	var err error
	if doesDbExist(testDbPath) {
		db = openDatabase(testDbPath)
	} else {
		db, err = createDatabase(testDbPath)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println(db)

	f, err := os.Open(baseDir)
	if err != nil {
		panic(err)
	}

	fileNames, err := f.Readdirnames(-1)
	if err != nil {
		panic(err)
	}

	files := getPartialHashes(fileNames)

	sort.Slice(
		files, func(i, j int) bool {
			return files[i].HashPartial < files[j].HashPartial
		},
	)

	var prev *File
	for _, file := range files {
		if prev == nil {
			prev = file
			continue
		}

		if prev.HashPartial == file.HashPartial {
			handleIdenticalFiles(prev, file)
		}

		prev = file
	}
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

func getPartialHashes(fileNames []string) []*File {
	var files []*File

	for _, fileName := range fileNames {
		file := &File{Path: path.Join(baseDir, fileName)}

		hash, err := calculateHash(file.Path, PartialHash)
		if err != nil {
			panic(err)
		}
		file.HashPartial = hash

		files = append(files, file)
	}
	return files
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

package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const (
	emptyFolder     = "/home/per/code/findDuplicates/test/empty"
	testFolder      = "/home/per/code/findDuplicates/test/walk"
	existsDbPath    = "/home/per/code/findDuplicates/test/data/exists.db"
	notExistsDbPath = "/home/per/code/findDuplicates/test/data/notExists.db"
)

func Test_doesDbExist(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"test.db", args{existsDbPath}, true},
		{"test2.db", args{notExistsDbPath}, false},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				if got := doesDbExist(tt.args.path); got != tt.want {
					t.Errorf("doesDbExist() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func Test_createDatabase(t *testing.T) {
	db, err := createDatabase(testDbPath)
	if err != nil {
		t.Error(err)
		return
	}
	if db == nil {
		t.Error("db should not be null")
		return
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)
	defer func() {
		err = os.Remove(testDbPath)
		if err != nil {
			t.Error(err)
		}
	}()

	const tempPath = "/home/per/temp/test.txt"
	err = insertFileWithHash(db, tempPath, "hash 123")
	if err != nil {
		t.Error(err)
		return
	}

	file, err := getFile(db, tempPath)
	if err != nil {
		t.Error(err)
		return
	}

	if file.Path != tempPath {
		t.Error("invalid path in getPath")
		return
	}
	if file.Hash != "hash 123" {
		t.Error("invalid hash in getPath")
		return
	}
}

func Test_findDuplicates(t *testing.T) {
	db, err := createDatabase(testDbPath)
	if err != nil {
		t.Error(err)
		return
	}
	if db == nil {
		t.Error("db should not be null")
		return
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)
	defer func() {
		err = os.Remove(testDbPath)
		if err != nil {
			t.Error(err)
		}
	}()

	err = insertFileWithHash(db, testDbPath, "123")
	if err != nil {
		t.Error(err)
		return
	}
	err = insertFileWithHash(db, testDbPath+"2", "123")
	if err != nil {
		t.Error(err)
		return
	}

	dup, err := findDuplicates(db)
	if err != nil {
		t.Error(err)
		return
	}

	for _, file := range dup {
		fmt.Println(file.Path, file.Hash)
	}
}

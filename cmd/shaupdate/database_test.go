package main

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
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
	err = insertFile(db, tempPath)
	if err != nil {
		t.Error(err)
		return
	}

	err = updatePartialHash(db, tempPath, "partial hash 123")
	if err != nil {
		t.Error(err)
		return
	}

	err = updateCompleteHash(db, tempPath, "complete hash 123")
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
	if file.HashComplete != "complete hash 123" {
		t.Error("invalid complete hash in getPath")
		return
	}
	if file.HashPartial != "partial hash 123" {
		t.Error("invalid partial hash in getPath")
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

	err = insertFileWithHash(db, testDbPath, "123", "12345")
	if err != nil {
		t.Error(err)
		return
	}
	err = insertFileWithHash(db, testDbPath+"2", "123", "12345")
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
		fmt.Println(file.Path, file.HashPartial, file.HashComplete)
	}
}

func Test_getFilesInPath(t *testing.T) {
	type args struct {
		dir     string
		pattern string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"empty folder", args{"/home/per/temp/empty_folder", ".go$"}, []string{}},
		{"test folder", args{testDbFolder, ".go$"}, []string{"/home/per/code/shaupdate/test/walk/walk.go"}},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := scanPath(tt.args.dir, tt.args.pattern)
				if err != nil {
					t.Errorf("scanPath() returned an error : %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("scanPath() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

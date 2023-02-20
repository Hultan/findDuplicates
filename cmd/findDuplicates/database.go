package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

func doesDbExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func openDatabase(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		log.Fatal(err)
	}

	return db
}

func createDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	sts := `
CREATE TABLE files(
    filePath TEXT PRIMARY KEY,
    hashPartial TEXT
);
`
	_, err = db.Exec(sts)

	if err != nil {
		return nil, fmt.Errorf("failed to execute create database : %w", err)
	}

	err = createIndex(db)
	if err != nil {
		return nil, fmt.Errorf("failed to execute create index : %w", err)
	}

	return db, nil
}

func createIndex(db *sql.DB) error {
	sts := `
CREATE INDEX IF NOT EXISTS idx_partial_hash 
ON files (hashPartial);
`
	_, err := db.Exec(sts)

	if err != nil {
		return err
	}

	return nil
}

func insertFileWithHash(db *sql.DB, filePath, partialHash string) error {
	SQL := "INSERT INTO files(filePath,hashPartial) VALUES(?,?,?);"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return err
	}

	_, err = stm.Exec(filePath, partialHash)
	if err != nil {
		return err
	}

	return nil
}

func getFile(db *sql.DB, filePath string) (*File, error) {
	SQL := "SELECT filePath, hashPartial FROM files WHERE filePath=?;"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return nil, err
	}
	defer func(stm *sql.Stmt) {
		_ = stm.Close()
	}(stm)

	rows, err := stm.Query(filePath)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	rows.Next()
	var path string
	var partial, complete sql.NullString

	err = rows.Scan(&path, &partial, &complete)
	if err != nil {
		return nil, err
	}

	return &File{
		Path:        path,
		HashPartial: partial.String,
	}, nil
}

func findDuplicatePartialHashes(db *sql.DB) ([]*File, error) {
	SQL := `
SELECT filePath, hashPartial FROM files WHERE hashPartial in (
	SELECT hashPartial
	FROM files
	GROUP BY hashPartial
	HAVING COUNT(*) > 1
);
`
	stm, err := db.Prepare(SQL)
	if err != nil {
		return nil, err
	}
	defer func(stm *sql.Stmt) {
		_ = stm.Close()
	}(stm)

	rows, err := stm.Query()
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var result []*File

	for rows.Next() {
		var path string
		var partial, complete sql.NullString

		err = rows.Scan(&path, &partial, &complete)
		if err != nil {
			return nil, err
		}

		result = append(
			result, &File{
				Path:        path,
				HashPartial: partial.String,
			},
		)
	}

	return result, nil
}

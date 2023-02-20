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
    hash TEXT
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
CREATE INDEX IF NOT EXISTS idx_hash 
ON files (hash);
`
	_, err := db.Exec(sts)

	if err != nil {
		return err
	}

	return nil
}

func insertFileWithHash(db *sql.DB, filePath, hash string) error {
	SQL := "INSERT INTO files(filePath,hash) VALUES(?,?);"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return err
	}

	_, err = stm.Exec(filePath, hash)
	if err != nil {
		return err
	}

	return nil
}

func getFile(db *sql.DB, filePath string) (*File, error) {
	SQL := "SELECT filePath, hash FROM files WHERE filePath=?;"

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
	var hash sql.NullString

	err = rows.Scan(&path, &hash)
	if err != nil {
		return nil, err
	}

	return &File{
		Path: path,
		Hash: hash.String,
	}, nil
}

func findDuplicates(db *sql.DB) ([]*File, error) {
	SQL := `
SELECT filePath, hash FROM files WHERE hash in (
	SELECT hash
	FROM files
	GROUP BY hash
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
		var hash sql.NullString

		err = rows.Scan(&path, &hash)
		if err != nil {
			return nil, err
		}

		result = append(
			result, &File{
				Path: path,
				Hash: hash.String,
			},
		)
	}

	return result, nil
}

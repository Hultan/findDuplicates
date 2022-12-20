package main

import (
	"database/sql"
	"log"
	"os"
)

const databaseName = "files.db"

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
    hashPartial TEXT, 
    hashComplete TEXT
);
`
	_, err = db.Exec(sts)

	if err != nil {
		return nil, err
	}

	err = createIndex(db)
	if err != nil {
		return nil, err
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

func insertFileWithHash(db *sql.DB, filePath, partialHash, completeHash string) error {
	SQL := "INSERT INTO files(filePath,hashPartial, hashComplete) VALUES(?,?,?);"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return err
	}

	_, err = stm.Exec(filePath, partialHash, completeHash)
	if err != nil {
		return err
	}

	return nil
}

func existsFile(db *sql.DB, filePath string) (bool, error) {
	SQL := "SELECT EXISTS(SELECT 1 FROM files WHERE filePath=?);"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return false, err
	}
	defer func(stm *sql.Stmt) {
		_ = stm.Close()
	}(stm)

	rows, err := stm.Query(filePath)
	if err != nil {
		return false, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	rows.Next()
	var exists bool

	err = rows.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func getFile(db *sql.DB, filePath string) (*File, error) {
	SQL := "SELECT filePath, hashPartial, hashComplete FROM files WHERE filePath=?;"

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
		Path:         path,
		HashComplete: complete.String,
		HashPartial:  partial.String,
	}, nil
}

func updateCompleteHash(db *sql.DB, filePath string, hash string) error {
	SQL := "UPDATE files SET hashComplete=? WHERE filePath=?;"

	stm, err := db.Prepare(SQL)
	if err != nil {
		return err
	}
	defer func(stm *sql.Stmt) {
		_ = stm.Close()
	}(stm)

	_, err = stm.Exec(hash, filePath)
	if err != nil {
		return err
	}

	return nil
}

func findDuplicates(db *sql.DB) ([]*File, error) {
	SQL := `
SELECT filePath, hashPartial, hashComplete FROM files WHERE hashPartial in (
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
				Path:         path,
				HashComplete: complete.String,
				HashPartial:  partial.String,
			},
		)
	}

	return result, nil
}

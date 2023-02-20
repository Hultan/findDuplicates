package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const hashSize = 65536

type hashedFile struct {
	hash string
	path string
	err  error
}

type fileList []string
type results map[string]fileList

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Missing parameter, provide dir name!")
	}

	fmt.Println("Scanning files...")
	fmt.Println()

	hashes, err := searchTree(os.Args[1])
	if err != nil {
		msg := fmt.Sprintf("Failed to search tree : %s", err)
		log.Fatal(msg)
	}

	for hash, files := range hashes {
		// Ignore non duplicates
		if len(files) == 1 {
			continue
		}

		// Report duplicate, we will use just 7 chars like git
		fmt.Printf("%s - %d files", hash[len(hash)-7:], len(files))
		for _, file := range files {
			fmt.Println("  ", file)
		}
		fmt.Println()
	}
}

func hashFile(path string) hashedFile {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Read the first section of the file
	b := make([]byte, hashSize)
	r := bufio.NewReader(f)
	n, err := r.Read(b)

	// Return MD5 hash of the file
	m := md5.New()
	return hashedFile{
		hash: fmt.Sprintf("%x", m.Sum(b[0:n])),
		path: path,
		err:  err,
	}
}

func searchTree(dir string) (results, error) {
	hashes := make(results)

	visit := func(p string, fi os.FileInfo, err error) error {
		if err != nil && err != os.ErrNotExist {
			return err
		}

		if fi.Mode().IsRegular() && fi.Size() > 0 {
			h := hashFile(p)

			// Report failed hashes, and ignore them
			if h.err != nil {
				fmt.Printf("Failed to hash file %s : %s", h.path, h.err)
				return nil
			}

			hashes[h.hash] = append(hashes[h.hash], h.path)
		}

		return nil
	}

	err := filepath.Walk(dir, visit)

	return hashes, err
}

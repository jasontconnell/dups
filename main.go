package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type file struct {
	name string
	hash string
	path string
	mod  time.Time
}

func main() {
	fld := flag.String("f", "", "folder to start at")
	chk := flag.Int("c", 15, "chunk size")
	ignore := flag.String("i", "", "ignore folder csv")
	flag.Parse()

	// byName := make(map[string][]file)
	byHash := make(map[string][]file)

	ign := make(map[string]string)
	if *ignore != "" {
		spl := strings.Split(*ignore, ",")
		for _, s := range spl {
			ign[s] = s
		}
	}

	dirs := []string{}
	filepath.Walk(*fld, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}

			if _, ok := ign[info.Name()]; ok {
				return filepath.SkipDir
			}

			dirs = append(dirs, path)
		}
		return nil
	})

	chunks := len(dirs) / *chk
	if len(dirs)%*chk != 0 {
		chunks++
	}

	log.Println("got dirs", len(dirs), "processing with", chunks, "chunks")
	var wg sync.WaitGroup
	wg.Add(chunks)

	chfile := make(chan file, 100000)

	for i := 0; i < chunks; i++ {
		start := i * *chk
		end := (i + 1) * *chk
		if end > len(dirs) {
			end = len(dirs)
		}

		log.Println("Starting chunk", i, " -> start, end =", start, end)
		go func(index int, chunk []string, ch chan file) {
			count := 0
			for _, ff := range chunk {
				entries, err := os.ReadDir(ff)
				if err != nil {
					log.Println("couldn't read dir", err)
					return
				}

				count += len(entries)
				for _, entry := range entries {
					info, err := entry.Info()

					if err != nil || info == nil {
						log.Println("---------nil info for", ff, err)
						continue
					}

					if info.IsDir() {
						continue
					}

					path := filepath.Join(ff, entry.Name())
					contents, err := os.ReadFile(path)
					if err != nil {
						log.Println("couldn't read file ", path)
						continue
					}

					hash := sha256sum(contents)

					cf := file{name: info.Name(), hash: hash, path: path, mod: info.ModTime()}
					chfile <- cf
				}
				//log.Println("done reading ", ff)
			}
			log.Println("done reading", count, "files in chunk", index)
			wg.Done()
		}(i, dirs[start:end], chfile)
	}

	wg.Wait()
	close(chfile)
	log.Println("read files:", len(chfile))

	for f := range chfile {
		byHash[f.hash] = append(byHash[f.hash], f)
	}

	for h, fs := range byHash {
		if len(fs) == 1 {
			continue
		}

		fmt.Println("Hash:", h)
		for _, f := range fs {
			fmt.Println(fmt.Sprintf("Name: %s Hash: %s Path: %s Mod Time: %s", f.name, f.hash, f.path, f.mod.String()))
		}

		fmt.Println()
	}

}

func sha256sum(d []byte) string {
	h := sha256.New()
	h.Write(d)
	b := h.Sum(nil)

	return fmt.Sprintf("%x", b)
}

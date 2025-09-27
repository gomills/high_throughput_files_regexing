package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Script's arguments
var DIRECTORY = "."
var preCompiledRegex = regexp.MustCompile(`mailto:[^"]+@gmail\.com`) // What to look for
var FILESEXTENSION = ".html"                                         // Extension of files that we want to regex
var CHUNKSIZE = 64                                                   // Default. Chunk size of buffered reading for regex
var CARRYOVERSIZE = 100                                              // You should modify this. Number of bytes to append to chunks to avoid streaming slicing the regex pattern

func main() {

	start := time.Now()

	// List directory
	entries, err := os.ReadDir(DIRECTORY)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Started pipeline")

	ctx, cancel := context.WithCancel(context.Background())

	// Launch a single worker to fill filepaths channel with files to regex
	filePathsCh := gatherFileNames(ctx, entries)

	// Listen to filepaths channel to regex them
	emailCh := extractRegex(ctx, filePathsCh)

	// Catch the immediately returned error if creating the output file fails and cancel all the pipeline
	err = saveEmailsToFile(emailCh)
	if err != nil {
		cancel()
	}

	log.Printf("Total time: %.3f seconds\n", time.Since(start).Seconds())

}

func gatherFileNames(ctx context.Context, entries []os.DirEntry) <-chan string {

	filePathsCh := make(chan string, min(len(entries), 150))

	go func() {

		for _, entry := range entries {

			if err := ctx.Err(); err != nil {
				break
			}

			if entry.IsDir() {
				continue
			}

			if strings.HasSuffix(entry.Name(), FILESEXTENSION) {
				filepath := filepath.Join(DIRECTORY, entry.Name())
				filePathsCh <- filepath
			}

		}

		close(filePathsCh)

	}()

	return filePathsCh

}

func extractRegex(ctx context.Context, filePathsCh <-chan string) <-chan string {

	findingsCh := make(chan string, 50)
	var wg sync.WaitGroup

	for range runtime.NumCPU() {

		wg.Add(1)
		go func() {

			defer wg.Done()

			for filename := range filePathsCh {

				if err := ctx.Err(); err != nil {
					break
				}

				file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
				if err != nil {
					log.Println(err)
					continue
				}

				// Deprecated line by line approach
				// scanner := bufio.NewScanner(file)

				// for scanner.Scan() {
				// 	line := scanner.Text()

				// 	if matches := preCompiledRegex.FindAll([]byte(line), -1); len(matches) > 0 {
				// 		for _, mc := range matches {
				// 			findingsCh <- string(mc)
				// 		}
				// 	}
				// }

				reader := bufio.NewReader(file)
				chunk := make([]byte, CHUNKSIZE*1024)
				appendix := []byte{}

			regexing:
				for {

					if err := ctx.Err(); err != nil {
						break
					}

					n, err := reader.Read(chunk)

					if n > 0 {

						data := append(appendix, chunk[:n]...)
						if matches := preCompiledRegex.FindAll(data, -1); len(matches) > 0 {
							for _, mc := range matches {
								select {
								case findingsCh <- string(mc):
									continue
								case <-ctx.Done():
									break regexing
								}
							}
						}

						if len(data) < CARRYOVERSIZE {
							appendix = data
						} else {
							appendix = data[len(data)-CARRYOVERSIZE:]
						}

					}

					if err != nil {

						if err != io.EOF {
							log.Println("Read error:", err)
						}

						break
					}
				}

				file.Close()
			}

		}()
	}

	go func() {
		wg.Wait()
		close(findingsCh)
	}()

	return findingsCh
}

func saveEmailsToFile(emailCh <-chan string) error {

	file, err := os.OpenFile("output.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	i := 0
	for email := range emailCh {

		_, err := writer.WriteString(email + "\n")
		if err != nil {
			log.Println(err)
		}

		if i%50 == 0 {
			err = writer.Flush()
			if err != nil {
				log.Println(err)
			}
		}

		i++

	}

	// Don't forget to flush!
	err = writer.Flush()
	if err != nil {
		log.Println(err)
	}
	return nil
}

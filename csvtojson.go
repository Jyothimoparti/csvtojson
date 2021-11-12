package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type inputFile struct {
	fpath     string
	separator string
	pretty    bool
}

func getFileContent() (inputFile, error) {

	if len(os.Args) < 2 {
		fmt.Println("Flags & CSV Path missing")
		os.Exit(1)
	}

	separator := flag.String("separator", "comma", "CSV file data separtor - comma/semicloumn")
	pretty := flag.Bool("pretty", false, "For a pretty JSON format")

	flag.Parse()

	//fmt.Printf("Separator: %s, Pretty : %t Tail: %v\n", *separator, *pretty, flag.Args())

	if len(flag.Args()) < 1 {
		fmt.Println("CSV file missing")
		os.Exit(1)
	}

	filePath := flag.Arg(0)

	//fmt.Println(filePath)

	if !(*separator == "comma" || *separator == "semicolon") {
		//return inputFile{}, errors.New("Only comma or semicolon separators are allowed")
		fmt.Println("Only comma or semicolon separators are allowed")
		os.Exit(1)
	}

	return inputFile{filePath, *separator, *pretty}, nil

}

func checkValidCSVFile(filename string) (bool, error) {
	fileExtension := filepath.Ext(filename)
	//fmt.Println(fileExtension)
	if fileExtension != ".csv" {
		fmt.Println("CSV file required, File is not CSV -", filename)
		os.Exit(1)
	}

	_, err := os.Stat(filename)
	//fmt.Println(fileExists, err)
	if os.IsNotExist(err) {
		return false, fmt.Errorf("File %s doesnot exists", filename)
	}

	return true, nil
}

func processCSV(fileContent inputFile, writerChannel chan<- map[string]string) {
	file, err := os.Open(fileContent.fpath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	//fmt.Println(file)
	defer fClose(file)

	var headers, line []string

	reader := csv.NewReader(file)

	headers, err = reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	for {
		line, err = reader.Read()

		if err == io.EOF {
			close(writerChannel)
			break
		}

		if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(line)
		record, err := processLine(headers, line)
		fmt.Println(record)
		if err != nil {
			fmt.Printf("Error: %v", err)
			continue
		}

		writerChannel <- record
	}
}

func processLine(headers, dataSlice []string) (map[string]string, error) {

	if len(dataSlice) != len(headers) {
		return nil, errors.New("Mismatch of row length with header length...Skipping")
	}

	recordMap := make(map[string]string)

	for i, name := range headers {
		recordMap[name] = dataSlice[i]
	}

	//fmt.Println(recordMap)

	return recordMap, nil
}

func writeJSONFile(csvPath string, writerChannel <-chan map[string]string, done chan<- bool, pretty bool) {
	writerString := createStringWriter(csvPath)
	jsonFunc, breakLine := getJSONFunc(pretty)

	fmt.Println("Writng to JSON file")

	writerString("["+breakLine, false)
	first := true
	for {
		record, more := <-writerChannel
		if more {
			if !first {
				writerString(","+breakLine, false)
			} else {
				first = false
			}

			jsonData := jsonFunc(record)
			writerString(jsonData, false)
		} else {
			writerString(breakLine+"]", true)
			fmt.Println("Completed")
			done <- true
			break
		}

	}

}

func createStringWriter(csvPath string) func(string, bool) {

	jsonDir := filepath.Dir(csvPath)
	//fmt.Println(jsonDir)
	jsonName := fmt.Sprintf("%s.json", strings.TrimSuffix(filepath.Base(csvPath), ".csv"))
	//fmt.Println(jsonName)
	jsonLocation := filepath.Join(jsonDir, jsonName)
	//fmt.Println(jsonLocation)

	f, err := os.Create(jsonLocation)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	return func(data string, close bool) {
		_, err := f.WriteString(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		if close {
			f.Close()
		}
	}
}

func getJSONFunc(pretty bool) (func(map[string]string) string, string) {
	var jsonFunc func(map[string]string) string
	var breakLine string
	if pretty {
		breakLine = "\n"
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.MarshalIndent(record, "   ", "   ")
			return "   " + string(jsonData)
		}
	} else {
		breakLine = ""
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.Marshal(record)
			return string(jsonData)
		}

	}
	return jsonFunc, breakLine
}

func fClose(file *os.File) {
	err := file.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] <csvFile>\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}
	fileContent, err := getFileContent()
	//fmt.Println(fileContent, err)
	if err != nil {
		os.Exit(1)
	}
	if _, err := checkValidCSVFile(fileContent.fpath); err != nil {
		os.Exit(1)
	}

	writerChannel := make(chan map[string]string)
	done := make(chan bool)

	go processCSV(fileContent, writerChannel)
	go writeJSONFile(fileContent.fpath, writerChannel, done, fileContent.pretty)
	<-done
}

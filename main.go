package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const DataPath = "./data"
const DataExtension = ".csv"
const ResultFile = "./result.csv"

const ThreadsLimit = 4
const RecordSetLimit = 1000
const GroupLimit = 20

const FloatSize = 64

func main() {

	data := make(chan *Content)
	var handlers sync.WaitGroup
	var sources sync.WaitGroup
	channel := &Listener{
		data:    data,
		records: &handlers,
		files:   &sources,
	}

	var output []Content
	var size = 0

	go func() {
		for {
			record, retrieve := <-data
			if retrieve {
				channel.records.Done()

				/* Считаем сколько одинаковых */
				var sames = getSames(record, output)
				/* Если одинаковых уже предельное количество,
				то заменяем */
				replaceSameWithCheap(sames, output, record)
				/* Если можно добавить элементы, то добавляем */
				size, output = appendToOrdinal(
					size,
					sames,
					output,
					record,
				)
				/* Если общее количество достигло предела,
				то заменяем */
				replaceOrdinalWithCheap(size, sames, output, record)
			}
		}
	}()

	parseData(channel)
	sortOutput(output)
	saveResult(output)
}

func saveResult(output []Content) {
	csvFile, err := os.Create(ResultFile)
	if err != nil {
		panic(err)
	}
	defer func(csvFile *os.File) {
		err := csvFile.Close()
		if err != nil {

		}
	}(csvFile)

	csvWriter := csv.NewWriter(csvFile)
	for i := range output {
		var id = strconv.Itoa(output[i].id)
		var price = strconv.FormatFloat(
			output[i].price,
			'f',
			-1,
			FloatSize)
		err = csvWriter.Write([]string{id, price})
		if err != nil {
			return
		}
	}
	csvWriter.Flush()
}

func parseData(channel *Listener) {
	files := getFilePaths()
	var limit = len(files)
	for i := 0; i < limit; i = i + ThreadsLimit {
		for j := 1; j < ThreadsLimit; j++ {
			if i+j < limit {
				channel.files.Add(1)
				go channel.extract(files[i+j])
			}
		}
		channel.files.Add(1)
		channel.extract(files[i])
	}

	channel.files.Wait()
	channel.records.Wait()
}

func sortOutput(output []Content) {
	for i := range output {
		for j := range output {
			if i > j && output[i].price < output[j].price {
				var buffer = output[i]
				output[i] = output[j]
				output[j] = buffer
			}
		}
	}
}

func getFilePaths() []string {
	var files []string

	err := filepath.Walk(
		DataPath,
		func(path string, info os.FileInfo, err error,
		) error {

			if !info.IsDir() && filepath.Ext(path) == DataExtension {
				files = append(files, path)
			}
			return nil
		})
	if err != nil {
		panic(err)
	}
	return files
}

func appendToOrdinal(
	size int,
	sames *Sames,
	output []Content,
	record *Content,
) (int, []Content) {
	if size != RecordSetLimit {
		/* Если новые добавить можно */
		/* и если одинаковых ещё не 20,
		то добавляем новый */
		if sames.count != GroupLimit {
			output = append(output, *record)
			size++
		}
	}
	return size, output
}

func replaceOrdinalWithCheap(
	size int,
	sames *Sames,
	output []Content,
	record *Content,
) {
	if size == RecordSetLimit &&
		sames.count != GroupLimit {
		/* Если новые добавить нельзя */
		/* Если можно добавить ещё одинаковых, */
		/* то ищем индекс самого дорогого из всех */
		var biggest = getBiggest(output)
		/* и дорогой меняем на дешёвый */
		output[biggest].price = record.price
		output[biggest].id = record.id
	}
}

func replaceSameWithCheap(
	sames *Sames,
	output []Content,
	record *Content,
) {
	if sames.count == GroupLimit &&
		sames.hasBigger {
		/* ищем индекс самого дорогого */
		var biggest = getBiggest(sames.elements)

		var realIndex = sames.indexes[biggest]
		/* дорогой меняем на дешёвый */
		output[realIndex].price = record.price
	}
}

type Content struct {
	id    int
	price float64
}
type Listener struct {
	data    chan *Content
	records *sync.WaitGroup
	files   *sync.WaitGroup
}

func (channel *Listener) extract(file string) {

	f, _ := os.Open(file)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	csvReader := csv.NewReader(f)
	for {

		values, err := csvReader.Read()
		if err == io.EOF {
			channel.files.Done()
			break
		}
		id, _ := strconv.Atoi(values[0])
		price, _ := strconv.ParseFloat(values[1], FloatSize)

		content := Content{id: id, price: price}
		channel.records.Add(1)
		channel.data <- &content
	}
}

type Sames struct {
	elements  []Content
	indexes   []int
	count     int
	hasBigger bool
}

func getSames(same *Content, output []Content) *Sames {
	var elements []Content
	var indexes []int
	var count = 0
	var hasBigger = false

	var identity = same.id
	var price = same.price

	for i := range output {
		var isSame = false
		if identity == output[i].id {
			elements = append(elements, output[i])
			indexes = append(indexes, i)
			isSame = true
			count++
		}
		if !hasBigger && isSame {
			hasBigger = output[i].price > price
		}
	}

	result := Sames{
		elements:  elements,
		indexes:   indexes,
		count:     count,
		hasBigger: hasBigger,
	}

	return &result
}

func getBiggest(recordset []Content) int {
	var biggest = 0
	for i := range recordset {
		if recordset[i].price > recordset[biggest].price {
			biggest = i
		}
	}

	return biggest
}

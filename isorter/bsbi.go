package isorter

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/sbwhitecap/tqdm"
	. "github.com/sbwhitecap/tqdm/iterators"
	"github.com/slow_extract/index"
	"github.com/slow_extract/mapper"
	"github.com/slow_extract/search"
	"github.com/slow_extract/stemmer"
	"github.com/slow_extract/utils"
)

type Bsbi struct {
	TermId    mapper.Id
	FileId    mapper.Id
	IndexPath string
	Stemmer stemmer.Stemmer 	
	SearchQuery search.SearchQuery
}

func (bsbi *Bsbi) CreateCollectionIndex(collectionPath string) (*index.InvertedIndexIterator, error) {
	folderCollection, err := os.Open(collectionPath)

	if err != nil {
		return nil, err
	}

	blockPaths, err := folderCollection.Readdirnames(0)

	if err != nil {
		return nil, err
	}

	invertedIndexHeap := index.InitHeap()
	indicesName := make([]string, 0)
	var iterationError error = nil

	tqdm.With(Strings(blockPaths), "Iterating Collections", func(v interface{}) (brk bool) {
		blockPath := v.(string)
		invertedIndex, termFrequencies, err := bsbi.parseBlock(collectionPath, blockPath)

		if err != nil {
			iterationError = err
			return true
		}
		indexName := fmt.Sprintf("i-%s", blockPath)
		indicesName = append(indicesName, indexName)
		indexWriter := index.InvertedIndex{}
		indexWriter.Init(indexName, bsbi.IndexPath)
		if err := indexWriter.Write(invertedIndex, termFrequencies); err != nil {
			iterationError = err
			return true
		}
		return
	})

	if iterationError != nil {
		return nil, iterationError
	}
	bsbi.TermId.Save(bsbi.IndexPath, "term")
	bsbi.FileId.Save(bsbi.IndexPath, "file")
	indicesReader := make([]*index.InvertedIndex, 0)

	for _, indexName := range indicesName {
		indexReader := index.InvertedIndex{}
		indicesReader = append(indicesReader, &indexReader)
		indexReader.Init(indexName, bsbi.IndexPath)
		invertedIndexHeap.Push(indexReader.Iterator())
	}
	defer bsbi.deleteIndices(indicesReader)
	return bsbi.mergeIndices(invertedIndexHeap)
}

func (bsbi *Bsbi) Search(query string) []string{
	queries := bsbi.Stemmer.StemSentence(query)
	terms := make([]uint32, 0)
	termErr := bsbi.TermId.Load(bsbi.IndexPath, "term")	
	fileErr := bsbi.FileId.Load(bsbi.IndexPath, "file")
	filePositions := make([]string, 0)
	if termErr != nil {
		panic(termErr)
	}
	
	if fileErr != nil {
		panic(fileErr)
	}


	mainIndex := index.InvertedIndex{}
	mainIndex.Init("main", bsbi.IndexPath)
	
	for _, q := range(queries) {
		terms = append(terms, bsbi.TermId.ToUint32(q))
	}

	
	searchResult :=  bsbi.SearchQuery.Search(terms)

	for _, fileId := range searchResult{
		str, err := bsbi.FileId.ToString(fileId)
		if err != nil {
			continue
		}
		filePositions = append(filePositions,str)
	}	
	return filePositions
}

func (bsbi *Bsbi) deleteIndices(indices []*index.InvertedIndex) {
	for _, ii := range indices {
		ii.Delete()
	}
}

func mergeTermFrequency(f1, f2 map[uint32]uint32) map[uint32]uint32 {
	mergedTermFrequency := make(map[uint32]uint32)
	
	for k1,v1 := range f1 {
		if v2, ok:= f2[k1]; ok {
			mergedTermFrequency[k1] = v1 + v2 
		} else {
			mergedTermFrequency[k1] = v1
		}

	}
	
	for k2,v2 := range f2 {
		if _, ok := mergedTermFrequency[k2]; !ok {
			mergedTermFrequency[k2] = v2
		}
	}


	return mergedTermFrequency 
}

func (bsbi *Bsbi) mergeIndices(invertedIndexHeap heap.Interface) (*index.InvertedIndexIterator, error) {
	indexWriter := index.InvertedIndex{}
	indexWriter.Init("main", bsbi.IndexPath)

	termPostingLists := make([][]uint32, 0)
	var smallestTerm uint32 = 0
	var currentTermFrequency map[uint32]uint32 = make(map[uint32]uint32)

	for {
		smallestElement := heap.Pop(invertedIndexHeap)
		switch smallestElement.(type) {
		case error:
			// there might be a case where termPostingLists isn't merged
			indexWriter.CloseIndex()
			indexWriter.WriteMetadata()
			return indexWriter.Iterator(), nil
		}

		smallestIterator := smallestElement.(*index.InvertedIndexIterator)
		nextSmallestTerm, _, smallestPostingList, nextTermFrequency, err := smallestIterator.Next()
		
		if err != nil {
			log.Println(err.Error())
			smallestIterator.IndexFile.Close()
			continue
		}

		if smallestTerm < nextSmallestTerm {
			mergedPostingList := utils.MergePostingLists(termPostingLists)
			indexWriter.WriteIndex(smallestTerm, currentTermFrequency, mergedPostingList)
			termPostingLists = make([][]uint32, 0)
			currentTermFrequency = make(map[uint32]uint32)
			smallestTerm = nextSmallestTerm
		}

		currentTermFrequency = mergeTermFrequency(currentTermFrequency, nextTermFrequency) 
		termPostingLists = append(termPostingLists, smallestPostingList)

		heap.Push(invertedIndexHeap, smallestIterator)
	}

}

func (bsbi *Bsbi) parseBlock(collectionPath, blockPath string) (map[uint32][]uint32, map[uint32]map[uint32]uint32, error) {
	fullBlockPath := fmt.Sprintf("%s/%s", collectionPath, blockPath)
	blockFolder, err := os.Open(fullBlockPath)
	invertedIndex := make(map[uint32][]uint32)
	termFrequencies := make(map[uint32]map[uint32]uint32)
	tdPairs := make([][]uint32, 0)

	if err != nil {
		return nil, nil, err
	}

	defer blockFolder.Close()

	filePaths, err := blockFolder.Readdirnames(0)

	if err != nil {
		return nil, nil, err
	}

	tqdm.With(Strings(filePaths), "Iterating text files", func(v interface{}) (brk bool) {

		filePath := v.(string)
		fullFilePath := fmt.Sprintf("%s/%s", fullBlockPath, filePath)
		file, err := os.OpenFile(fullFilePath, os.O_RDONLY, 0755)
		if err != nil {
			log.Println(err)
			file.Close()
			return
		}
		buffer := bufio.NewScanner(file)
		mappedFilePath := bsbi.FileId.ToUint32(filePath)

		for buffer.Scan() {
			texts :=  bsbi.Stemmer.StemSentence(buffer.Text())

			for _, text := range texts {
				mappedTerm := bsbi.TermId.ToUint32(text)
				tdPair := make([]uint32, 2)
				tdPair[0], tdPair[1] = mappedTerm, mappedFilePath
				tdPairs = append(tdPairs, tdPair)
			}
		}

		file.Close()

		return
	})

	sort.Slice(tdPairs, func(i, j int) bool {
		if tdPairs[i][0] == tdPairs[j][0] {
			return tdPairs[i][1] < tdPairs[j][1]
		}
		return tdPairs[i][0] < tdPairs[j][0]
	})

	for _, tdPair := range tdPairs {
		if _, ok := invertedIndex[tdPair[0]]; !ok {
			invertedIndex[tdPair[0]] = make([]uint32, 0)
			invertedIndex[tdPair[0]] = append(invertedIndex[tdPair[0]], tdPair[1])
			termFrequencies[tdPair[0]] = make(map[uint32]uint32)
		}

		if _, ok := termFrequencies[tdPair[0]][tdPair[1]]; !ok {
			termFrequencies[tdPair[0]][tdPair[1]] = 0
		}

		termFrequencies[tdPair[0]][tdPair[1]] +=1 

		n := uint32(len(invertedIndex[tdPair[0]]))

		if (n > 0 && invertedIndex[tdPair[0]][n-1] != tdPair[1]) {
			invertedIndex[tdPair[0]] = append(invertedIndex[tdPair[0]], tdPair[1])
		}

	}

	return invertedIndex, termFrequencies, nil
}

package isorter

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/sbwhitecap/tqdm"
	. "github.com/sbwhitecap/tqdm/iterators"
	"github.com/slow_extract/index"
	"github.com/slow_extract/mapper"
	"github.com/slow_extract/utils"
)

type Bsbi struct {
	TermId    mapper.Id
	FileId    mapper.Id
	IndexPath string
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
		invertedIndex, err := bsbi.parseBlock(collectionPath, blockPath)

		if err != nil {
			iterationError = err
			return true
		}
		indexName := fmt.Sprintf("i-%s", blockPath)
		indicesName = append(indicesName, indexName)
		indexWriter := index.InvertedIndex{}
		indexWriter.Init(indexName, bsbi.IndexPath)

		if err := indexWriter.Write(invertedIndex); err != nil {
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

func (bsbi *Bsbi) deleteIndices(indices []*index.InvertedIndex) {
	for _, ii := range indices {
		ii.Delete()
	}
}

func (bsbi *Bsbi) mergeIndices(invertedIndexHeap heap.Interface) (*index.InvertedIndexIterator, error) {
	indexWriter := index.InvertedIndex{}
	indexWriter.Init("main", bsbi.IndexPath)

	termPostingLists := make([][]uint32, 0)
	var smallestTerm uint32 = 0

	for {
		smallestElement := heap.Pop(invertedIndexHeap)
		switch smallestElement.(type) {
		case error:
			indexWriter.CloseIndex()
			indexWriter.WriteMetadata()
			return indexWriter.Iterator(), nil 
		}

		smallestIterator := smallestElement.(*index.InvertedIndexIterator)
		nextSmallestTerm, smallestPostingList, err := smallestIterator.Next()
		if err != nil {
			log.Println(err.Error())
			smallestIterator.IndexFile.Close()
			continue
		}

		if smallestTerm == nextSmallestTerm {
			termPostingLists = append(termPostingLists, smallestPostingList)
		} else {
			mergedPostingList := utils.MergePostingLists(termPostingLists)
			indexWriter.WriteIndex(nextSmallestTerm, mergedPostingList)
			smallestElement = nextSmallestTerm
		}

		heap.Push(invertedIndexHeap, smallestIterator)
	}
}

func (bsbi *Bsbi) parseBlock(collectionPath, blockPath string) (map[uint32][]uint32, error) {
	fullBlockPath := fmt.Sprintf("%s/%s", collectionPath, blockPath)
	blockFolder, err := os.Open(fullBlockPath)
	invertedIndex := make(map[uint32][]uint32)
	tdPairs := make([][]uint32, 0)

	if err != nil {
		return nil, err
	}

	defer blockFolder.Close()

	filePaths, err := blockFolder.Readdirnames(0)

	if err != nil {
		return nil, err
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
			texts := strings.Split(buffer.Text(), " ")

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
		}

		if invertedIndex[tdPair[0]][len(invertedIndex[tdPair[0]])-1] < tdPair[1] {
			invertedIndex[tdPair[0]] = append(invertedIndex[tdPair[0]], tdPair[1])
		}
	}

	return invertedIndex, nil
}

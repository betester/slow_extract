package isorter

import (
	"bufio"
	"container/heap"
	"fmt"
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

func (bsbi *Bsbi) CreateCollectionIndex(collectionPath string) error {
	folderCollection, err := os.Open(collectionPath)

	if err != nil {
		return err
	}

	blockPaths, err := folderCollection.Readdirnames(0)

	if err != nil {
		return err
	}

	invertedIndexHeap := index.InitHeap()
	var iterationError error = nil

	tqdm.With(Strings(blockPaths), "Iterating Collections", func(v interface{}) (brk bool) {
		blockPath := v.(string)
		invertedIndex, err := bsbi.parseBlock(collectionPath, blockPath)

		if err != nil {
			iterationError = err
			return true
		}
		indexName := fmt.Sprintf("i-%s", blockPath)
		indexWriter := index.InvertedIndex{}
		indexWriter.Init(indexName, bsbi.IndexPath)
		invertedIndexHeap.Push(indexWriter.Iterator())

		if err := indexWriter.Write(invertedIndex); err != nil {
			iterationError = err
			return true
		}
		return
	})

	if iterationError != nil {
		return iterationError
	}

	return bsbi.mergeIndices(invertedIndexHeap)
}

func (bsbi *Bsbi) mergeIndices(invertedIndexHeap heap.Interface) error {
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
			return nil
		}

		smallestIterator := smallestElement.(index.InvertedIndexIterator)
		nextSmallestTerm, smallestPostingList, err := smallestIterator.Next()

		if err != nil {
			continue
		}

		if smallestTerm == nextSmallestTerm {
			termPostingLists = append(termPostingLists, smallestPostingList)
		} else {
			mergedPostingList := utils.MergePostingLists(termPostingLists)
			indexWriter.WriteIndex(smallestTerm, mergedPostingList)
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

	filePaths, err := blockFolder.Readdirnames(0)

	if err != nil {
		return nil, err
	}

	var iterationErr error = nil

	tqdm.With(Strings(filePaths), "Iterating text files", func(v interface{}) (brk bool) {

		filePath := v.(string)
		fullFilePath := fmt.Sprintf("%s/%s", fullBlockPath, filePath)
		file, err := os.OpenFile(fullFilePath, os.O_RDONLY, 0755)

		if err != nil {
			iterationErr = err
			return true
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

		sort.Slice(tdPairs, func(i, j int) bool {
			return tdPairs[i][1] <= tdPairs[j][1]
		})

		for _, tdPair := range tdPairs {
			if _, ok := invertedIndex[tdPair[0]]; !ok {
				invertedIndex[tdPair[0]] = make([]uint32, 0)
			}

			invertedIndex[tdPair[0]] = append(invertedIndex[tdPair[0]], tdPair[1])
		}

		return
	})

	if iterationErr != nil {
		return nil, iterationErr
	}

	return invertedIndex, nil
}

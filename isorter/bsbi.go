package isorter

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"github.com/slow_extract/index"
	"github.com/slow_extract/mapper"
)

type Bsbi struct {
	TermId mapper.Id
	FileId mapper.Id
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
	indexNames := make([]string, 0)
	for _, blockPath := range blockPaths {

		invertedIndex, err  := bsbi.parseBlock(collectionPath, blockPath)
		if err != nil {
			return err
		}
		indexName := fmt.Sprintf("i-%s",blockPath)
		indexWriter := index.InvertedIndex{}
		indexWriter.Init(indexName, bsbi.IndexPath)
		indexNames = append(indexNames, indexName)

		if err := indexWriter.Write(invertedIndex); err != nil {
			return err
		}
	}

	return mergeIndices(indexNames)
}

func mergeIndices(indices []string) error {
	return nil	
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

	for _, filePath := range filePaths {
		fullFilePath := fmt.Sprintf("%s/%s", fullBlockPath, filePath)
		file, err := os.OpenFile(fullFilePath, os.O_RDONLY, 0755)

		if err != nil {
			return nil, err
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

	return invertedIndex, nil
} 
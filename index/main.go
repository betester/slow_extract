package index

import "github.com/slow_extract/compressor"

type InvertedIndex struct {
	indexName string
	indexPath string
	encoder compressor.Compressor
	terms []uint32
}

func (ii *InvertedIndex) Init()

func (ii *InvertedIndex) Next() (uint32, []uint32) {

}
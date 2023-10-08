package search

import (
	"math"
	"sort"

	"github.com/slow_extract/index"
	"github.com/slow_extract/utils"
	"golang.org/x/exp/slices"
)

type SearchQuery interface {
	Search([]uint32) []uint32
}

type CosineSearch struct {
	K int 
	Iterator index.InvertedIndexIterator
	N int32
}

func (cs *CosineSearch) tf(term, document uint32, termFrequencies map[uint32]uint32) uint32 {
	if val, docExist := termFrequencies[document]; docExist {
		return val
	}
	return 0
}

func (cs *CosineSearch) wtd(term, document uint32, termFrequencies map[uint32]uint32) float64 {
	termFrequency := cs.tf(term, document, termFrequencies)
	
	if termFrequency > 0 {
		return 1 + math.Log10(float64(termFrequency))
	}

	return 0
}

func (cs *CosineSearch) wtq(documentFrequency uint32) float64 {
	return math.Log10(float64(cs.N/int32(documentFrequency)))
}

func (cs *CosineSearch) Search(query []uint32) []uint32 {
	topKDoc := make([]uint32, 0)
	scores := make(map[uint32]float64)
	scoresPair := make([]utils.Pair[uint32, float64], 0)

	for cs.Iterator.HasNext() {
		term, docFreq, decodedPostingList, termFrequency, _ := cs.Iterator.Next()

		if contain := slices.Contains(query, term); contain {
			for _, doc := range decodedPostingList {
				docScore := cs.wtd(term, doc, termFrequency) * cs.wtq(docFreq)

				if _, ok := scores[doc]; !ok {
					scores[doc] = 0
				}

				scores[doc] += docScore
			}
		}
	}

	for doc, score := range scores {
		scoresPair = append(scoresPair, utils.Pair[uint32, float64]{First: doc, Second: score})
	}


	sort.Slice(scoresPair, func(i,j int) bool {
		return scoresPair[i].Second < scoresPair[j].Second
	})

	for i:=0; i < cs.K; i++ {
		topKDoc = append(topKDoc, scoresPair[i].First)
	}

	return topKDoc
}

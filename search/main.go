package search

import (
	"math"
	"sort"

	"github.com/slow_extract/index"
	"github.com/slow_extract/utils"
	"golang.org/x/exp/slices"
)

type SearchQuery interface {
	Search(query []uint32) []uint32
}

type CosineSearch struct {
	K int 
	Iterator index.InvertedIndexIterator
	N int32
}

type IntersectionSearch struct {
	Iterator index.InvertedIndexIterator
}

func (cs CosineSearch) tf(term, document uint32, termFrequencies map[uint32]uint32) uint32 {
	if val, docExist := termFrequencies[document]; docExist {
		return val
	}
	return 0
}

func (cs CosineSearch) wtd(term, document uint32, termFrequencies map[uint32]uint32) float64 {
	termFrequency := cs.tf(term, document, termFrequencies)
	
	if termFrequency > 0 {
		return 1 + math.Log10(float64(termFrequency))
	}

	return 0
}

func (cs CosineSearch) wtq(documentFrequency uint32) float64 {
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

func (is IntersectionSearch) findPostingListIntersection(pl1, pl2 []uint32) []uint32 {
	p1, p2 := 0, 0
	intersectedPl := make([]uint32, 0)	

	for p1 < len(pl1) && p2 < len(pl2) {

		if pl1[p1] == pl2[p2] {
			intersectedPl=  append(intersectedPl, pl1[p1])	
			p1++
			p2++
		} else if pl1[p1] > pl2[p2] {
			p2++
		} else {
			p1++
		}
	}

	return intersectedPl
}



func (is *IntersectionSearch) Search(query []uint32) []uint32 {
	iterator := is.Iterator 
	queryPostingList := make([][]uint32, 0)
	
	for iterator.HasNext() {
		term, _, postingList, _, _ := iterator.Next()
		if slices.Contains(query, term) {
			queryPostingList = append(queryPostingList, postingList)
		}
	}

	intersectedPostingList := make([]uint32, 0)	
	
	if len(queryPostingList) == 0 {
		return intersectedPostingList 
	}

	intersectedPostingList = append(intersectedPostingList, queryPostingList[0]...)
	
	for i:=1; i < len(queryPostingList); i++ {
		intersectedPostingList = is.findPostingListIntersection(intersectedPostingList, queryPostingList[i])
	}
	
	return intersectedPostingList
}

package utils

import (
	"container/heap"
	"fmt"
)

type SortedPostingList struct {
	PostingList []uint32
	Pointer     int
}

type SortedPostingListHeap []*SortedPostingList

func (spl SortedPostingListHeap) Len() int {
	return len(spl)
}

func (spl SortedPostingListHeap) Less(i, j int) bool {
	pi, pj := spl[i].Pointer, spl[j].Pointer
	pli, plj := spl[i].PostingList, spl[j].PostingList

	return pli[pi] > plj[pj]
}

func (spl SortedPostingListHeap) Swap(i, j int) {

	if spl.Len() == 0 {
		return
	}

	spl[i], spl[j] = spl[j], spl[i]
}

func (spl *SortedPostingListHeap) Push(x any) {
	newElement := x.(*SortedPostingList)
	*spl = append(*spl, newElement)
}

func (spl *SortedPostingListHeap) Pop() any {

	if spl.Len() == 0 {
		return fmt.Errorf("heap is empty")
	}

	old := *spl
	n := len(*spl) - 1
	iterator := old[0]
	old[0] = old[n]
	old[n] = nil
	*spl = old[0:n]

	return iterator
}

func InitHeap(postingLists [][]uint32) heap.Interface {
	invertedIndexHeap := make(SortedPostingListHeap, 0)
	heap.Init(&invertedIndexHeap)

	for _, postingList := range postingLists {
		spl := SortedPostingList{PostingList: postingList, Pointer: 0}
		heap.Push(&invertedIndexHeap, &spl)
	}

	return &invertedIndexHeap
}

func MergePostingLists(postingLists [][]uint32) []uint32 {

	mergedPostingLists := make([]uint32, 0)
	heapPostingList := InitHeap(postingLists)

	for {
		smallestElement := heap.Pop(heapPostingList)

		switch smallestElement.(type) {
		case error:
			return mergedPostingLists
		}

		smallestPostingList := smallestElement.(*SortedPostingList)
		smallesNumber := smallestPostingList.PostingList[smallestPostingList.Pointer]
		mergedPostingLists = append(mergedPostingLists, smallesNumber)
		smallestPostingList.Pointer++

		if smallestPostingList.Pointer < len(smallestPostingList.PostingList) {
			heap.Push(heapPostingList, smallestPostingList)
		}

	}
}



package index

import (
	"container/heap"
	"fmt"
)

type InvertedIndexHeap []*InvertedIndexIterator

func (iih InvertedIndexHeap) Len() int{
	return len(iih)
}

func (iih InvertedIndexHeap) Less(i, j int) bool {
	termI, _, _ := iih[i].Current()
	termJ, _, _ := iih[j].Current()
	
	return termI < termJ
}

func (iih InvertedIndexHeap) Swap(i, j int) {
	iih[i], iih[j] = iih[j], iih[i]
}

func (iih *InvertedIndexHeap) Push(x any) {
	iterator := x.(InvertedIndexIterator)
	*iih = append(*iih, &iterator)
}

func (iih *InvertedIndexHeap) Pop() any {

	if iih.Len() == 0 {
		return fmt.Errorf("heap is empty")
	} 

	old := *iih
	n := len(*iih) - 1
	iterator := old[0]
	old[0] = old[n-1]
	old[n-1] = nil
	*iih = old[0 : n-1] 
	
	return iterator
}

func InitHeap() heap.Interface {
	invertedIndexHeap := make(InvertedIndexHeap, 0)
	heap.Init(&invertedIndexHeap)

	return &invertedIndexHeap
}
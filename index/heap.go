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
	termI,_, _,_, _ := iih[i].Current()
	termJ,_, _,_, _ := iih[j].Current()
	
	return termI < termJ
}

func (iih InvertedIndexHeap) Swap(i, j int) {

	if iih.Len() == 0 {
		return
	} 

	iih[i], iih[j] = iih[j], iih[i]
}

func (iih *InvertedIndexHeap) Push(x any) {
	iterator := x.(*InvertedIndexIterator)
	*iih = append(*iih, iterator)
}

func (iih *InvertedIndexHeap) Pop() any {

	if iih.Len() == 0 {
		return fmt.Errorf("heap is empty")
	} 

	old := *iih
	n := len(*iih) - 1
	iterator := old[n]
	*iih = old[0 : n] 

	return iterator
}

func InitHeap() heap.Interface {
	invertedIndexHeap := make(InvertedIndexHeap, 0)
	heap.Init(&invertedIndexHeap)

	return &invertedIndexHeap
}

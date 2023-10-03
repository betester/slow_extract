package utils

import (
	"container/heap"
)

// Define a custom type for the heap elements to hold the value and the list index.
type HeapNode struct {
	value uint32
	index int // Index of the list from which the value is taken
	pointer int
}

// Define a min-heap based on the HeapNode type.
type MinHeap []HeapNode

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapNode))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func MergePostingLists(postingLists [][]uint32) []uint32 {
	// Create a min-heap to efficiently merge the posting lists.
	minHeap := make(MinHeap, len(postingLists))
	result := []uint32{}

	// Initialize the min-heap with the first element from each posting list.
	for i, list := range postingLists {
		if len(list) > 0 {
			minHeap[i] = HeapNode{value: list[0], index: i, pointer: 0}
		}
	}

	// Heapify the min-heap.
	heap.Init(&minHeap)

	// Merge the posting lists using the min-heap.
	for minHeap.Len() > 0 {
		// Pop the minimum element from the heap.
		min := heap.Pop(&minHeap).(HeapNode)
		result = append(result, min.value)

		// If there are more elements in the same list, push the next element to the heap.
		if min.pointer < len(postingLists[min.index]) {
			heap.Push(&minHeap, HeapNode{value: postingLists[min.index][min.pointer + 1], index: min.index, pointer: min.pointer + 1})
		}
	}

	return result
}

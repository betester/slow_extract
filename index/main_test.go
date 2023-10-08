package index

import (
	"fmt"
	"reflect"
	"testing"
)

func TestInvertedIndex(t *testing.T) {
	mappedDocument := make(map[uint32][]uint32)
	mappedDocument[1] = make([]uint32, 0)
	mappedDocument[1] = append(mappedDocument[1], 3, 5, 8, 10)
	mappedDocument[2] = make([]uint32, 0)
	mappedDocument[2] = append(mappedDocument[2], 1, 3, 5, 7)
	
	termFrequencies := make(map[uint32]map[uint32]uint32)
	termFrequencies[1] = make(map[uint32]uint32)
	termFrequencies[2] = make(map[uint32]uint32)

	termFrequencies[1][3] = 10
	termFrequencies[1][5] = 1
	termFrequencies[1][8] = 7
	termFrequencies[1][10] = 2

	termFrequencies[2][1] = 10
	termFrequencies[2][3] = 1
	termFrequencies[2][5] = 7
	termFrequencies[2][7] = 2

	invertedIndex := InvertedIndex{}
	invertedIndex.Init("test", ".tmp")
	invertedIndex.Write(mappedDocument, termFrequencies)

	iterator := invertedIndex.Iterator()

	mappedIndex := make(map[uint32][]uint32)	

	for iterator.HasNext() {
		term, _, postingList, termFrequency, err := iterator.Next()

		if err != nil {
			t.Errorf(err.Error())
		}

		if equal := reflect.DeepEqual(termFrequency, termFrequencies[term]); !equal {
			fmt.Println("indexed file not equal with document", mappedDocument, mappedDocument)
			t.FailNow()
		}

		mappedIndex[term] = postingList
	}

	if equal := reflect.DeepEqual(mappedIndex, mappedDocument); !equal {
		fmt.Println("indexed file not equal with document", mappedDocument, mappedDocument)
		t.FailNow()
	}

}


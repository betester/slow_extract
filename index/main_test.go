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

	invertedIndex := InvertedIndex{}
	invertedIndex.Init("test", ".tmp")
	invertedIndex.Write(mappedDocument)

	iterator := invertedIndex.Iterator()

	mappedIndex := make(map[uint32][]uint32)	

	for iterator.HasNext() {
		term, postingList, err := iterator.Next()

		if err != nil {
			t.Errorf(err.Error())
		}

		mappedIndex[term] = postingList
	}

	if equal := reflect.DeepEqual(mappedIndex, mappedDocument); !equal {
		fmt.Println("indexed file not equal with document", mappedDocument, mappedDocument)
		t.FailNow()
	}

}


package compressor 

import (
	"fmt"
	"reflect"
	"testing"
)

func TestVariableByteEncoder(t *testing.T) {
	numbers := [6]uint32{0, 34, 67, 89, 454, 2345738}
	vbe := VariableByteEncoder{}
	totalByteSize := 0
	for _, n := range numbers {
		encodedNumber := vbe.Encode(n)
		fmt.Printf("%v", encodedNumber)
		decodedNumber, _:= vbe.Decode(encodedNumber, 0)
		totalByteSize += len(encodedNumber)
		if equal := reflect.DeepEqual(n, decodedNumber); !equal {
			t.Error(fmt.Sprintf("Encoding error, result not equal %d != %d", n, decodedNumber))
		}
	}

	fmt.Println("Compressed size", totalByteSize)
}

func TestPostingListEncoder(t *testing.T) {
	numbers := [6]uint32{0, 34, 67, 89, 454, 2345738}
	plc := PostingListCompressor{Compress: &VariableByteEncoder{}}
	encodeResult := plc.Encode(numbers[:])   
	result := plc.Decode(encodeResult)

	if equal := reflect.DeepEqual(numbers[:], result); !equal {
		t.Errorf("Decode result is not equal %v != %v", numbers, result)	
	}
}

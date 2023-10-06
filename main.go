package main

import (
	"fmt"
	"log"

	"github.com/slow_extract/isorter"
	"github.com/slow_extract/mapper"
)

func main() {
	bsbi := isorter.Bsbi{
		TermId:    mapper.Id{
			ListOfId: []string{},
			MapOfId:  map[string]uint32{},
		},
		FileId:    mapper.Id{
			ListOfId: []string{},
			MapOfId:  map[string]uint32{},
		},
		IndexPath: "ci",
	} 

	iterator , err := bsbi.CreateCollectionIndex("collections")

	if err != nil {
		fmt.Println(err)
	}

	for iterator.HasNext() {
		log.Println(iterator.Next())
	}
}
package main

import (
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

	query := "apa"

	result := bsbi.Search(query)
	
	for _, file := range result {
		log.Println(file)
	}

}

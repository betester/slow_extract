package main

import (
	"log"

	"github.com/RadhiFadlillah/go-sastrawi"
	"github.com/slow_extract/isorter"
	"github.com/slow_extract/mapper"
	"github.com/slow_extract/stemmer"
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
		Stemmer: &stemmer.SastrawiStemmer{Dictionary: sastrawi.DefaultDictionary()},
	} 

	query := "risiko kesehatan bahaya"
	result := bsbi.Search(query)
	
	for _, file := range result {
		log.Println(file)
	}

}

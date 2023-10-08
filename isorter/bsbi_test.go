package isorter_test

import (
	"log"
	"testing"

	"github.com/RadhiFadlillah/go-sastrawi"
	"github.com/slow_extract/isorter"
	"github.com/slow_extract/mapper"
	"github.com/slow_extract/search"
	"github.com/slow_extract/stemmer"
)

func TestBsbi(t *testing.T) {
	bsbi := isorter.Bsbi{
		TermId:    mapper.Id{
			ListOfId: []string{},
			MapOfId:  map[string]uint32{},
		},
		FileId:    mapper.Id{
			ListOfId: []string{},
			MapOfId:  map[string]uint32{},
		},
		IndexPath: ".tmp",
		Stemmer: &stemmer.SastrawiStemmer{Dictionary: sastrawi.DefaultDictionary()},
	}

	iterator , err := bsbi.CreateCollectionIndex("koleksi")
	if err != nil {
		log.Println(err)
	}

	for iterator.HasNext() {
		log.Println(iterator.Next())
	}

	bsbi.SearchQuery = &search.CosineSearch{
		K:2,

	}
	result := bsbi.Search("bebek")
	for _, file := range result {
		log.Println(file)
	}

}

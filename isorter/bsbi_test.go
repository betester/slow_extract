package isorter_test

import (
	"log"
	"testing"

	"github.com/slow_extract/isorter"
	"github.com/slow_extract/mapper"
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
	}

	itrator, err := bsbi.CreateCollectionIndex("koleksi")
	if err != nil {
		log.Println(err)
	} else {
		for itrator.HasNext() {
			term, _, _ := itrator.Next()
			log.Println(term)
		}
	}
}
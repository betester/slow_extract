package index

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/slow_extract/compressor"
)

type invertedIndexIterator struct {
	Terms []uint32
	PostingListMap map[uint32][]uint32
	IndexFile *os.File
	Index int
	Decoder compressor.PostingListCompressor
}

func (iii *invertedIndexIterator) HasNext() bool {
	return iii.Index < len(iii.Terms)
}

func (iii * invertedIndexIterator) Next() (uint32, []uint32, error) {
	if iii.HasNext() {
		term := iii.Terms[iii.Index]
		offset, byteLength := iii.PostingListMap[term][0], iii.PostingListMap[term][2]
		encodedPostingList := make([]byte, byteLength)

		if _, err := iii.IndexFile.Seek(int64(offset), 0); err != nil {
			log.Fatalln(err.Error())
		}
		
		if _, err := iii.IndexFile.Read(encodedPostingList); err != nil {
			log.Fatalln(err.Error())
		}
		decodedPostingList := iii.Decoder.Decode(encodedPostingList)

		iii.Index++
		return term, decodedPostingList, nil
	}
	iii.IndexFile.Close()
	return 0, nil, fmt.Errorf("end of iterator")
}

type InvertedIndex struct {
	indexName string
	indexPath string
	indexFile *os.File 
	encoder compressor.PostingListCompressor
	terms []uint32
	postingListMap map[uint32][]uint32
	currentOffset uint32
}

type invertedIndexMetadata struct {
	Terms []uint32
	PostingListMap map[uint32][]uint32 
}

func (ii *InvertedIndex) openMetadata() invertedIndexMetadata{

	indexMetadataFilePath := fmt.Sprintf("%s/%s.json", ii.indexPath, ii.indexName)
	metadataFile, err := os.OpenFile(indexMetadataFilePath, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Fatalln("Failed to open index metadata file")
	}
	
	defer metadataFile.Close()
	defer log.Println("Metadata closed")

	var metadata invertedIndexMetadata;
	metadataDecoder := json.NewDecoder(metadataFile)
	
	err = metadataDecoder.Decode(&metadata)
	
	if err != nil {
		log.Println(err.Error())
		metadata = invertedIndexMetadata{
			Terms:         make([]uint32, 0), 
			PostingListMap: make(map[uint32][]uint32),
		}
	}
	
	log.Println("Metadata opened")
	return metadata
}

func (ii *InvertedIndex) openIndex() *os.File {
	indexFilePath := fmt.Sprintf("%s/%s.index", ii.indexPath, ii.indexName)
	indexFile, err := os.OpenFile(indexFilePath, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Fatalln("Failed to open index file")
	}
	log.Println("Index opened")
	return indexFile
}

func (ii *InvertedIndex) writeIndex(term uint32, postingList []uint32) error {
	encodedPostingList := ii.encoder.Encode(postingList) 

	if _,err := ii.indexFile.Write(encodedPostingList); err != nil {
		return err
	}

	ii.postingListMap[term] = make([]uint32, 0)
	ii.postingListMap[term] = append(ii.postingListMap[term], ii.currentOffset)
	ii.postingListMap[term] = append(ii.postingListMap[term], uint32(len(postingList)))
	ii.postingListMap[term] = append(ii.postingListMap[term], uint32(len(encodedPostingList)))
	ii.terms = append(ii.terms, term)
	ii.currentOffset += uint32(len(encodedPostingList))
	
	return nil
}

func (ii *InvertedIndex) writeMetadata() error {
	
	indexMetadataFilePath := fmt.Sprintf("%s/%s.json", ii.indexPath, ii.indexName)
	metadataFile, err := os.OpenFile(indexMetadataFilePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalln("Failed to open index metadata file")
	}
	
	defer metadataFile.Close()
	defer log.Println("Metadata closed")

	metadata := invertedIndexMetadata{Terms: ii.terms, PostingListMap: ii.postingListMap}
	metadataEncoder := json.NewEncoder(metadataFile)
	return metadataEncoder.Encode(metadata)	
}

func (ii *InvertedIndex) Init(indexName, indexPath string) {
	ii.indexName = indexName
	ii.indexPath = indexPath
	ii.encoder = compressor.PostingListCompressor{Compress: &compressor.VariableByteEncoder{}}	
	ii.terms = make([]uint32, 0)
	ii.postingListMap = make(map[uint32][]uint32)

	err := os.Mkdir(indexPath, 0750)

	if err != nil && !os.IsExist(err) {
		log.Fatal(err)		
	} else if err != nil && os.IsExist(err) {
		log.Println("Folder already created")
	} else {
		log.Println("Folder created!")
	}

	indexFile := ii.openIndex()
	metadata := ii.openMetadata()

	ii.indexFile = indexFile
	ii.terms = metadata.Terms
	ii.postingListMap = metadata.PostingListMap
}

func (ii *InvertedIndex) Iterator() invertedIndexIterator {
	metadata := ii.openMetadata()
	ii.indexFile = ii.openIndex()

	return invertedIndexIterator{
		Terms: metadata.Terms, 
		PostingListMap: metadata.PostingListMap,
		Decoder: ii.encoder,
		IndexFile: ii.indexFile,
	}
} 

func (ii *InvertedIndex) Write(mappedDoc map[uint32][]uint32) error {

	for term, postingList := range mappedDoc {
		if err := ii.writeIndex(term, postingList); err != nil {
			return err
		}
	}

	ii.writeMetadata()
	ii.indexFile.Close()
	return nil
}
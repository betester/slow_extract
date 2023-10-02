package mapper

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Id struct {
	ListOfId []string
	MapOfId map[string]uint32
}

func (id *Id) ToUint32(key string) uint32 {
	if _, ok := id.MapOfId[key]; !ok {
		id.MapOfId[key] = uint32(len(id.ListOfId))
		id.ListOfId = append(id.ListOfId, key)
	}

	return id.MapOfId[key]
}

func (id *Id) ToString(key uint32) (string, error) {
	if len(id.ListOfId) <= int(key) {
		return "", fmt.Errorf("key %d doesn't exist", key)	
	}
	return id.ListOfId[key], nil
}

func (id *Id) Load(folder, filename string) error {
	idFilePath := fmt.Sprintf("%s/%s.json", folder, filename)
	idFile, err := os.OpenFile(idFilePath, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Println("Failed to open " + idFilePath)
		return err
	}
	
	idDecoder := json.NewDecoder(idFile)
	return idDecoder.Decode(id)
}

func (id *Id) Save(folder, filename string) error {
	idFilePath := fmt.Sprintf("%s/%s.json", folder, filename)

	err := os.Mkdir(folder, 0750)

	if err != nil && !os.IsExist(err) {
		log.Fatal(err)		
	} else if err != nil && os.IsExist(err) {
		log.Println("Folder already created")
	} else {
		log.Println("Folder created!")
	}
	idFile, err := os.OpenFile(idFilePath, os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Println("Failed to open " + idFilePath)
		return err
	}
	idEncoder := json.NewEncoder(idFile)
	return idEncoder.Encode(id)
}
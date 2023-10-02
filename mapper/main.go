package mapper

import "fmt"

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
package mapper

import (
	"fmt"
	"testing"
)

func TestID(t *testing.T) {
	test := "hello";
	idMapper := Id{ListOfId: make([]string, 0), MapOfId: make(map[string]uint32)}
	helloNumber := idMapper.ToUint32(test)

	if helloNumber != 0 {
		fmt.Println("the number should start from 0")
		t.FailNow()
	}

	if val, _ :=idMapper.ToString(helloNumber); val!= test {
		fmt.Println("mapping value not equal", test, val)
		t.FailNow()
	}
}
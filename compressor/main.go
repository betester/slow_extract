package compressor


type Compressor interface {
	Encode(n uint32) []byte
	Decode(encodedNumber []byte, startinIndex int) (uint32, int)
}

type VariableByteEncoder struct {
}

type PostingListCompressor struct {
	Compress Compressor
}

func (plc *PostingListCompressor) Encode(n []uint32) []byte {
	result := make([]byte, 0)

	gapPostingList := make([]uint32, 0)
	gapPostingList = append(gapPostingList, n[0])

	for i := 1; i < len(n); i++ {
		gapPostingList = append(gapPostingList, n[i]-n[i-1])
	}
	for i := 0; i < len(gapPostingList); i++ {
		result = append(result, plc.Compress.Encode(gapPostingList[i])...)
	}
	return result
}

func (plc *PostingListCompressor) Decode(bytes []byte) []uint32 {
	result := make([]uint32, 0)
	i := 0
	for i < len(bytes) {
		n, nextI := plc.Compress.Decode(bytes, i)
		i = nextI + 1
		result = append(result, n)
	}


	gaplessPostingList := make([]uint32, 0)

	var accumulatedSum uint32 = 0
	for i := 0; i < len(result); i++ {
		accumulatedSum += result[i]
		gaplessPostingList = append(gaplessPostingList, accumulatedSum)
	}

	return gaplessPostingList
}

func (vbe *VariableByteEncoder) Encode(n uint32) []byte {
	result := make([]byte, 0)
	for {
		result = append([]byte{byte(n % 128)}, result...)
		if n < 128 {
			break
		}
		n = n / 128
	}
	
	result[len(result) - 1] += byte(128)
	return result
}

func (vbe *VariableByteEncoder) Decode(encodedNumber []byte, startinIndex int) (uint32, int) {
	var decodedNumber uint32 = 0
	for {

		if startinIndex >= len(encodedNumber) {
			break
		}
		numberRepr := uint8(encodedNumber[startinIndex])
		if numberRepr < 128 {
			decodedNumber = 128*decodedNumber + uint32(numberRepr)
		} else {
			decodedNumber = 128*decodedNumber + uint32(numberRepr) - 128
			break
		}
		startinIndex++
	}

	return decodedNumber, startinIndex
}

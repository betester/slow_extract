package utils

func Insert(a [][]uint32, index int, value []uint32) [][]uint32 {
    a = append(a[:index+1], a[index:]...) // Step 1+2
    a[index] = value                      // Step 3
    return a
}
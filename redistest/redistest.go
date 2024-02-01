package main

import (

	"fmt"
	"strconv"
)

func main() {
	hexString := strconv.FormatInt(int64(10), 16)
	fmt.Println(hexString)
}

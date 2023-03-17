package main

import (
	"fmt"
)

type Person struct {
	name string
}

func main() {
	newMap := make(map[int]Person)
	a := Person{"bob"}
	newMap[1] = a
	fmt.Println(newMap[1])
	a = Person{"Amy"}
	fmt.Println(newMap[1])

}

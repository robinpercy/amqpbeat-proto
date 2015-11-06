package main

import (
	"fmt"
	"os"
	"bufio"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {
	file, err := os.Open("./info.json")
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		if i < 10 {
			fmt.Println(scanner.Text())
		}
		i++
	}
}

package main

import "fmt"

func main() {
	fmt.Println("Hello World by client!")

	var name string
	fmt.Print("Enter your name: ")
	fmt.Scanln(&name) // 从用户输入中读取字符串

	fmt.Printf("Hello, %s!\n", name)
}

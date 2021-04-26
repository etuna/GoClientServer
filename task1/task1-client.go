package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var reader *bufio.Reader
var user string
var serverAddr string

func main() {
	path, _ := os.Getwd()
	fmt.Println("[CLIENT] Working directory :" + path)

	if len(os.Args) != 3 {
		fmt.Println("Usage: ", os.Args[0], "server port")
		os.Exit(1)
	}

	ip := os.Args[1]   // Server ip
	port := os.Args[2] // Server port
	serverAddr = ip + ":" + port
	//conn, err := net.Dial("tcp", serverAddr) // connect to server

	var line string
	reader = bufio.NewReader(os.Stdin)
	fmt.Println("\n\n###   Welcome to DStoreLand   ###\n\n1)Enter the username\n2)Enter the filename to store\n3)Enter the filename to retrieve\n4)Exit")
	for {
		fmt.Print("Please select an option:")

		line, _ = reader.ReadString('\n')
		option := strings.TrimRight(line, "\r\n")

		handleRequest(serverAddr, option)
	}
}

func handleRequest(serverAddr string, option string) {

	switch option {
	case "1":
		// Login
		fmt.Print("Enter the username:")
		line, _ := reader.ReadString('\n')
		username := strings.TrimRight(line, "\r\n")
		login(serverAddr, username)
	case "2":
		// Upload
		fmt.Print("Enter the filename to store:")
		line, _ := reader.ReadString('\n')
		start := time.Now()
		fname := strings.TrimRight(line, "\r\n")
		upload(serverAddr, fname)
		end := time.Now()
		duration := end.Sub(start).Microseconds()
		fmt.Println("Upload Duration : " + fmt.Sprint(duration))
	case "3":
		// Download
		fmt.Print("Enter the filename to retrieve:")
		line, _ := reader.ReadString('\n')
		fname := strings.TrimRight(line, "\r\n")
		start := time.Now()
		download(fname)
		end := time.Now()
		duration := end.Sub(start).Microseconds()
		fmt.Println("Download Duration : " + fmt.Sprint(duration))
	case "4":
		os.Exit(0)
	default:
		// Default
		fmt.Println("default")
	}
}

func upload(serverAddr string, fname string) {
	// connect to server
	conn, _ := net.Dial("tcp", serverAddr)
	defer conn.Close()

	conn.Write([]byte("UPLOAD:" + fname + "\n"))

	// open file to upload
	fi, _ := os.Open(fname)
	defer fi.Close()

	// upload
	_, _ = io.Copy(conn, fi)
	fmt.Println("[CLIENT] " + fname + " stored successfully.")

}

func login(serverAddr string, username string) {
	conn, _ := net.Dial("tcp", serverAddr) // connect to server
	defer conn.Close()
	_, err2 := conn.Write([]byte("LOGIN:" + username + "\n"))
	//fmt.Fprintf(conn, "LOGIN:"+param)
	if err2 != nil {
		fmt.Println("Error writing to stream.")
	} else {
		clientReader := bufio.NewReader(conn)
		res, _ := clientReader.ReadString('\n')
		res = strings.TrimRight(res, "\r\n")
		fmt.Println(res)
	}
}

func download(file string) {
	conn, _ := net.Dial("tcp", serverAddr) // connect to server
	defer conn.Close()
	conn.Write([]byte("DOWNLOAD:" + file + "\n"))
	clientReader := bufio.NewReader(conn)
	request, _ := clientReader.ReadString('\n')
	request = strings.TrimRight(request, "\r\n")
	//fmt.Println("Answer found or not found:", request)
	if request == "OK" {
		// create new file
		dstFile := "./" + file
		fo, er2 := os.Create(dstFile)
		if er2 != nil {
			fmt.Println("er?", er2)
		}
		defer fo.Close()

		// accept file from client & write to new file
		_, er := io.Copy(fo, conn)
		if er != nil {
			fmt.Println("[SERVER] Error occured while processing upload request.", er)
		}
		fmt.Println("[CLIENT] File " + file + " found.")
	} else {
		fmt.Println("[CLIENT] File not found in your storage.")
	}

}

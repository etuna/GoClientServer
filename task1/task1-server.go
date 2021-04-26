package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

var authUser string

var server net.Listener

func main() {
	path, _ := os.Getwd()
	fmt.Println("[SERVER] Working directory :" + path)
	fmt.Println("[SERVER] Launching the server ...")

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "port")
		os.Exit(1)
	}

	port := os.Args[1] // Server port

	server, err := net.Listen("tcp", "127.0.0.1:"+port) // listening to incoming connections

	if err != nil {
		fmt.Println("[SERVER] nil1")
		// handle error
	}

	for { // loop forever (or until ctrl-c)FfF
		conn, err := server.Accept() // accept connection on port
		//fmt.Println(">Server Side| New Client connected.", conn)
		if err != nil {
			fmt.Println("[SERVER] nil2")
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	clientReader := bufio.NewReader(conn)
	request, _ := clientReader.ReadString('\n')
	fmt.Println("Request:", request)
	cmd, param := getCmd(request)

	switch cmd {
	case "LOGIN":
		conn.Write([]byte("[SERVER RESPONSE] Login successful\n"))
		authUser = strings.TrimRight(param, "\r\n")
		break
	case "UPLOAD":
		handleUploadReq(conn, strings.TrimRight(param, "\r\n"))
		break
	case "DOWNLOAD":
		handleDownloadReq(conn, param)
		break
	default:
		break
	}

}

func getCmd(request string) (string, string) {
	req := strings.Split(request, ":")
	cmd := req[0]
	param := req[1]
	return cmd, param
}

func handleUploadReq(conn net.Conn, file string) {

	path, _ := os.Getwd()
	destDir := path + "/" + authUser + "/"
	dstFile := destDir + file

	if _, dirErr := os.Stat(destDir); os.IsNotExist(dirErr) {
		os.Mkdir(destDir, 777)
	}

	// create new file
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
	fmt.Println("[SERVER] Upload request successfully processed")

}

func checkFileExists(conn net.Conn, dstFile string) int {

	// open file to upload
	_, err := os.Open(dstFile)
	fmt.Println(dstFile)
	if err != nil {
		conn.Write([]byte("NOTFOUND\n"))
		return 0
	} else {
		conn.Write([]byte("OK\n"))
		return 1
	}
}

func handleDownloadReq(conn net.Conn, file string) {
	path, _ := os.Getwd()
	destDir := path + "/" + authUser + "/"
	dstFile := destDir + strings.TrimRight(file, "\r\n")

	if checkFileExists(conn, dstFile) == 0 {
		//Do nothing
	} else {
		// open file to upload
		fi, _ := os.Open(dstFile)
		defer fi.Close()

		// upload
		_, _ = io.Copy(conn, fi)
		conn.Close()
		fmt.Println("[SERVER] Download request successfully processed")

	}

}

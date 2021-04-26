package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

type DHTService struct {
}

func (dht *DHTService) SignalStabilize(senderNode uint) error {
	fingerTableStabilize()
	if thisNode.GetSuccId() != senderNode {
		succPeerAddrPort := strings.Split(thisNode.Successor, ":")
		spAddr := succPeerAddrPort[0]
		spPort := succPeerAddrPort[1]
		spPPort, _ := strconv.Atoi(spPort)
		addressToAccess := spAddr + ":" + fmt.Sprint(spPPort+10)
		//	fmt.Println("SIGNAL STABLADDRESSTOACCESS :::" + addressToAccess)
		client, _ := rpc.Dial("tcp", addressToAccess) // connecting to the service
		client.Call("DHTService.SignalStabilize", &senderNode, nil)
		client.Close()
	}
	return nil

}
func handlePeerRequests(peerPort string) {
	// sarting server
	tcpAddr, er1 := net.ResolveTCPAddr("tcp", ":"+peerPort)
	if er1 != nil {
		fmt.Println("[PEER] handlePeerRequests, resolve tcp addr error: ", er1)
	}
	listener, er2 := net.ListenTCP("tcp", tcpAddr)

	if er2 != nil {
		fmt.Println("[PEER] handlePeerRequests, listen tcp error: ", er2)
	}
	fmt.Printf("\n[PEER HANDLER] %s is listening the address for peers:%s\n", thisNode.Id, tcpAddr)
	for {
		fmt.Printf("[PEER HANDLER] Listener ready to accept\n")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("[PEER] Handle Peer Request listener accept error")
			continue
		}
		rpc.ServeConn(conn)
	}

}

type SuccessorRequestModel struct {
	MaxPosAddr string
	NodeId     uint
}
type SuccessorResponseModel struct {
	SuccId   uint
	SuccAddr string
}
type InfoResponseModel struct {
	SuccId   uint
	SuccAddr string
	SuccFT   [2]string
	PredId   uint
	PredAddr string
}
type PredInfoResponseModel struct {
	PredId   uint
	PredAddr string
	PredFT   [2]string
}

func (dht *DHTService) Info(i *int, infoResModel *InfoResponseModel) error {
	infoResModel.SuccId = thisNode.SuccId
	infoResModel.SuccAddr = thisNode.Successor
	infoResModel.PredId = thisNode.PredId
	infoResModel.PredAddr = thisNode.Predecessor
	//fmt.Println("[PEER] Info ended")
	return nil
}

func (dht *DHTService) FindSuccRecurExt(succRequest *SuccessorRequestModel, succResponse *SuccessorResponseModel) error {
	//var reply1 int
	//err = client.Call("PS3.Multiply", &args, &reply1)
	//fmt.Println("[PEER] findSuccRecurExt dialing :" + succRequest.maxPosAddr)

	//	client.Call("DHTService.findSuccRecurExt", &succRequest, &succResponse)

	var maxPosId uint
	maxPosId = 0
	locationDetected := false
	//fmt.Println("maxpos - id - succreq.nodid:" + fmt.Sprint(maxPosId) + "-" + fmt.Sprint(id) + "-" + fmt.Sprint(succRequest.nodeId) + "-")
	//fmt.Println("[PEER] erken öten ?")

	for i := 0; i < 2; i++ {
		//	fmt.Println("[PEER] erken öten ?2")

		idAddr := strings.Split(thisNode.FingerTable()[i], "-")
		id64, _ := strconv.ParseUint(idAddr[0], 0, 32)
		id := uint(id64)
		addr := idAddr[1]
		var prevMaxPosId uint
		prevMaxPosId = 0
		//fmt.Println("maxpos - id - succreq.nodid:" + fmt.Sprint(maxPosId) + "-" + fmt.Sprint(id) + "-" + fmt.Sprint(succRequest.NodeId) + "-")
		if maxPosId <= id && succRequest.NodeId > id {
			prevMaxPosId = maxPosId
			maxPosId = id
			succRequest.MaxPosAddr = addr
			//	fmt.Println("[PEER] erken öten ?3")
		}

		if succRequest.NodeId < id && succRequest.NodeId > prevMaxPosId {
			locationDetected = true
		}

		if (succRequest.NodeId > id || thisNode.Id == 0) && maxPosId == id {
			fmt.Println("[PEER] erken öten ?4")
			maxPosId = id
			succRequest.MaxPosAddr = addr
			locationDetected = true
			break
		}
	}
	//fmt.Println("Location detected ? : " + fmt.Sprint(locationDetected))
	if !locationDetected {
		//fmt.Println("[PEER] erken öten ?5")

		client, err := rpc.Dial("tcp", succRequest.MaxPosAddr) // connecting to the service

		if err != nil {
			fmt.Println("[PEER] findSuccRecurExt, dial error.", err)
		}
		errr := client.Call("DHTService.FindSuccRecurExt", &succRequest, &succResponse)
		if errr != nil {
			fmt.Println("DHTService.FindSuccRecurExt error:", errr)
		}
		//fmt.Println("[PEER] erken öten ?7")
		client.Close()
		//dht.findSuccRecurExt(succRequest, succResponse)
	} else {
		//	fmt.Println("[PEER] erken öten ?6")
		succResponse.SuccId = maxPosId
		succResponse.SuccAddr = succRequest.MaxPosAddr
		//	fmt.Printf("LOCATION DETECTEEEEDDDD %s --- %s", fmt.Sprint(maxPosId), succRequest.MaxPosAddr)
	}
	//fmt.Println("[PEER] FindSuccRecurExt ended")
	return nil
}
func tempPlacePeerAddrToFT(peerAddr string) {
	for i := 0; i < 2; i++ {
		idAddr := strings.Split(thisNode.FingerTable()[i], "-")
		newEntry := idAddr[0] + "-" + peerAddr
		thisNode.Fingers[i] = newEntry
	}
}
func (dht *DHTService) DoJoin(peerAddr string, nodeId uint) {
	// 1) Look at finger table and find the closest node
	var maxPosId uint
	var maxPosAddr string
	maxPosId = 0
	maxPosAddr = ""
	locationDetected := false
	tempPlacePeerAddrToFT(peerAddr)
	for i := 0; i < 2; i++ {
		idAddr := strings.Split(thisNode.FingerTable()[i], "-")
		id64, _ := strconv.ParseUint(idAddr[0], 0, 32)
		id := uint(id64)
		maddr := idAddr[1]
		fmt.Println("maxposid - nodeid - id" + fmt.Sprint(maxPosId) + fmt.Sprint(nodeId) + "   " + fmt.Sprint(id))
		if maxPosId <= id && nodeId <= id {
			maxPosId = id
			maxPosAddr = maddr
		}
		//fmt.Println("[[[]]]nodeid - id" + fmt.Sprint(nodeId) + "   " + fmt.Sprint(id))
		if nodeId > id {
			fmt.Println("Location Detected : " + fmt.Sprint(maxPosId) + fmt.Sprint(nodeId) + "   " + fmt.Sprint(id))
			locationDetected = true
			break
		}
	}
	var succId uint
	var succAddr string
	succId = 0
	// 2) After determining the closest node, recursively ask for closest and greater id'ed node
	//fmt.Printf("[PEER] Before findSuccRecurExt, id:%s and addr:%s ", fmt.Sprint(nodeId), maxPosAddr)
	succRequest := SuccessorRequestModel{maxPosAddr, nodeId}
	succResponse := SuccessorResponseModel{succId, succAddr}
	if !locationDetected {
		dht.FindSuccRecurExt(&succRequest, &succResponse)
		fmt.Println("FindSuccRecurExt Response: " + succResponse.SuccAddr)
	} else {
		succId = maxPosId
		succAddr = maxPosAddr
	}
	fmt.Println(fmt.Sprint(succId) + " --- " + succAddr)
	// 3) At the end, retrieve addr:port, successor, predecessor, ids,fingertables
	if succId == 0 {
		succId = succResponse.SuccId
		succAddr = succResponse.SuccAddr
		thisNode.setSuccessor(succResponse.SuccAddr)
		thisNode.SuccId = succId
		thisNode.setPredecessor(succResponse.SuccAddr)
		thisNode.PredId = succId
		fmt.Println(fmt.Sprint(succResponse.SuccAddr) + " SUCC<AD HERE DR>PRED " + succResponse.SuccAddr)
		//	fmt.Println(fmt.Sprint(thisNode.predId) + " SUCC<I HERE D>PRED " + fmt.Sprint(thisNode.predId))
		/* 		thisNode.setSuccessor(succAddr)
		   		thisNode.succId = succId
		   		thisNode.setPredecessor(predAddr)
		   		thisNode.predId = predId */

	} else {
		client, err := rpc.Dial("tcp", succAddr) // connecting to the service
		fmt.Println(fmt.Sprint(succId) + " <> " + succAddr)
		if err != nil {
			fmt.Println("[PEER] findLocation, dial error.")
		}
		var infoRes InfoResponseModel
		var intm int
		client.Call("DHTService.Info", &intm, &infoRes)
		fmt.Println("DHT.INFO + " + fmt.Sprint(infoRes.SuccId))
		client.Close()
		client, err = rpc.Dial("tcp", infoRes.PredAddr) // connecting to the service

		if err != nil {
			fmt.Println("[PEER] findLocation, dial error.")
		}
		var predInfoRes PredInfoResponseModel
		client.Call("DHTService.Info", &intm, &predInfoRes)
		// Successor, pred, finger tables etcs
		fmt.Println(fmt.Sprint(infoRes.SuccAddr) + " SUCC<ADDR>PRED " + infoRes.PredAddr)
		fmt.Println(fmt.Sprint(infoRes.SuccId) + " SUCC<ID>PRED " + fmt.Sprint(infoRes.PredId))
		thisNode.setSuccessor(infoRes.SuccAddr)
		thisNode.SuccId = infoRes.SuccId
		thisNode.setPredecessor(infoRes.PredAddr)
		thisNode.PredId = infoRes.PredId
		client.Close()
	}
	fmt.Println("Finger Table Stabilize Starting...")
	fingerTableStabilize()
	//fmt.Println("fingertablestabilize ---> OK")

}

type SetSuccessorRequestModel struct {
	SuccId   uint
	SuccAddr string
}

func (dht *DHTService) SetSuccessorExt(setSuccessorRequestModel *SetSuccessorRequestModel, t *int) error {
	//fmt.Printf("[PEER] setSuccessorExt, %s, %s\n", fmt.Sprint(setSuccessorRequestModel.SuccId), setSuccessorRequestModel.SuccAddr)
	thisNode.SuccId = setSuccessorRequestModel.SuccId
	thisNode.setSuccessor(setSuccessorRequestModel.SuccAddr)
	*t = 1
	return nil
}

type SetPredecessorRequestModel struct {
	PredId   uint
	PredAddr string
}

func (dht *DHTService) SetPredecessorExt(setPredecessorRequestModel *SetPredecessorRequestModel, t *int) error {
	thisNode.PredId = setPredecessorRequestModel.PredId
	thisNode.setPredecessor(setPredecessorRequestModel.PredAddr)
	*t = 1
	return nil
}

type NodeFindRequestModel struct {
	Id   uint
	Addr string
}

type NodeFindResponseModel struct {
	ThatNodeId   uint
	ThatNodeAddr string
}

func (dht *DHTService) FindNodeForFingerTable(nodeFindRequestModel *NodeFindRequestModel, nodeFindResponseModel *NodeFindResponseModel) error {

	//client.Call("DHTService.findNodeForFingerTable", &nodeFindRequestModel, &nodeFindResponseModel)
	var tid uint
	tid = 0
	taddr := ""
	nodeFound := false
	for i := 0; i < 2; i++ {
		//fmt.Println(thisNode.FingerTable()[i])
		idAddr := strings.Split(thisNode.FingerTable()[i], "-")
		//fmt.Println(idAddr[0])
		id64, _ := strconv.ParseUint(idAddr[0], 0, 32)
		//fmt.Println("id64:" + fmt.Sprint(id64))
		tid = uint(id64)
		//fmt.Println("tid:" + fmt.Sprint(tid))

		taddr = idAddr[1]

		if nodeFindRequestModel.Id > tid {
			//fmt.Println("nodefindrequestmodelId>tid" + fmt.Sprint(nodeFindRequestModel.Id) + "  " + fmt.Sprint(tid))
		}
		if nodeFindRequestModel.Id <= tid || tid == 0 {
			//fmt.Println("nodefindrequestmodelId <== tid" + fmt.Sprint(nodeFindRequestModel.Id) + "  " + fmt.Sprint(tid))
			nodeFindRequestModel.Id = tid
			nodeFindRequestModel.Addr = taddr
			nodeFound = true
			break
		}
	}

	if !nodeFound {
		//fmt.Println("!nodeFound here " + fmt.Sprint(nodeFindRequestModel.Id))

		succPeerAddrPort := strings.Split(nodeFindRequestModel.Addr, ":")
		spAddr := succPeerAddrPort[0]
		spPort := succPeerAddrPort[1]
		spPPort, _ := strconv.Atoi(spPort)
		addressToAccess := spAddr + ":" + fmt.Sprint(spPPort+10)
		//fmt.Println("Dialing ... " + addressToAccess)
		client, err := rpc.Dial("tcp", addressToAccess) // connecting to the service

		if err != nil {
			fmt.Println("[PEER] findSuccRecurExt, dial error.", err)
		}

		client.Call("DHTService.FindNodeForFingerTable", &nodeFindRequestModel, &nodeFindResponseModel)
		//dht.findNodeForFingerTable(nodeFindRequestModel, nodeFindResponseModel)
		client.Close()
	} else {
		nodeFindResponseModel.ThatNodeId = nodeFindRequestModel.Id
		nodeFindResponseModel.ThatNodeAddr = nodeFindRequestModel.Addr
		//fmt.Println("node found : " + fmt.Sprint(nodeFindRequestModel.Id) + "  " + nodeFindRequestModel.Addr)
	}
	//fmt.Println("[PEER] FindNodeForFingerTable ended")
	return nil

}

func succPredStabilize() {

}

func fingerTableStabilize() {
	var dht DHTService
	// 1) Calculate 0 and 1 values
	zeroId := thisNode.Id + uint(math.Pow(2, 0))
	oneId := thisNode.Id + uint(math.Pow(2, 1))
	// 2) Look for 0 and 1 id-addr s

	var thatNodeId uint
	var thatNodeAddr string
	//fmt.Println("0th element for finger table starting....")
	nodeFindRequestModel0 := NodeFindRequestModel{zeroId, thisNode.Successor}
	nodeFindResponseModel0 := NodeFindResponseModel{thatNodeId, thatNodeAddr}
	dht.FindNodeForFingerTable(&nodeFindRequestModel0, &nodeFindResponseModel0)

	//fmt.Println("1st element for finger table starting....")
	nodeFindRequestModel1 := NodeFindRequestModel{oneId, thisNode.Successor}
	nodeFindResponseModel1 := NodeFindResponseModel{thatNodeId, thatNodeAddr}
	dht.FindNodeForFingerTable(&nodeFindRequestModel1, &nodeFindResponseModel1)
	//fmt.Println("FindNodeFingerTables completed. Ex: " + nodeFindResponseModel0.ThatNodeAddr + " " + nodeFindResponseModel1.ThatNodeAddr)
	// 3) update finger table entries
	id_addr0 := fmt.Sprint(nodeFindResponseModel0.ThatNodeId) + "-" + nodeFindResponseModel0.ThatNodeAddr
	//fmt.Println("id-add0 : " + id_addr0)
	id_addr1 := fmt.Sprint(nodeFindResponseModel1.ThatNodeId) + "-" + nodeFindResponseModel1.ThatNodeAddr
	//fmt.Println("id-add0 : " + id_addr1)
	thisNode.Fingers[0] = id_addr0
	thisNode.Fingers[1] = id_addr1

}

func (dht *DHTService) Join(addr string) {
	// 1) thisNode gets ready to join
	if addr == "START" {
		thisNode.SetId(0)
		for i := 0; i < 2; i++ {
			idAddr := strings.Split(thisNode.FingerTable()[i], "-")
			//id64, _ := strconv.ParseUint(idAddr[0], 0, 32)
			//id := uint(id64)
			addr := idAddr[1]
			newEntry := "0-" + addr
			thisNode.Fingers[i] = newEntry
		}
		fmt.Println("[PEER] Empty DHT, first peer")
		return
	} else {
		dht.DoJoin(addr, thisNode.Id)
	}
	// 2) ask successor and predecessor to do the arrangements(succ, pred, fing)
	//fmt.Printf("[PEER] join, thisNode successor is: %s\n", thisNode.Successor)
	succPeerAddrPort := strings.Split(thisNode.Successor, ":")
	spAddr := succPeerAddrPort[0]
	spPort := succPeerAddrPort[1]
	spPPort, _ := strconv.Atoi(spPort)
	addressToAccess := spAddr + ":" + fmt.Sprint(spPPort+10)
	//fmt.Println("ADDRESSTOACCESS :::" + addressToAccess)
	client, err := rpc.Dial("tcp", addressToAccess) // connecting to the service

	if err != nil {
		fmt.Println("[PEER] join, setSuccessor, dial error.")
	}
	//Ask predecessor first
	//fmt.Println("HERE")
	setSuccessorRequestModel := SetSuccessorRequestModel{thisNode.Id, thisNode.addr}
	//fmt.Println("HERE11")
	var t int
	err12 := client.Call("DHTService.SetSuccessorExt", &setSuccessorRequestModel, &t)
	//fmt.Println("HERE111")
	if err12 != nil {
		fmt.Println("SetSuccessorExt error Call", err12)
	}
	//fmt.Println("HERe2")
	//Ask successor
	setPredecessorRequestModel := SetPredecessorRequestModel{thisNode.Id, thisNode.addr}
	err13 := client.Call("DHTService.SetPredecessorExt", &setPredecessorRequestModel, &t)
	if err13 != nil {
		fmt.Println("SetPredecessorExt error Call")
	}
	fmt.Println("[PEER] Join, Completed..")
	// 3) file transfers if needed

	// 4) stabilize dht
	client.Close()
	dht.SignalStabilize(thisNode.Id)
}

func (dht *DHTService) Leave() {
	// 1) thisNode transfer the files to the successor

	// 2) ask successor and predecessors to do the arrangements (succ, pred, ids, etc)

	// 3) stabilize dht
}

type FileService struct {
}

func (fs *FileService) storeFile() {
	// 1) Retrieve the file temporarily from client

	// 2) Find the right node to store that file

	// 3) Transfer the file to that node

	// 4) Remove temp file
}

func (fs *FileService) sendFile() {
	// 1) Find the node that has the file

	// 2) Retrieve the file temporarily from that node

	// 3) Transfer the file to the client

	// 4) Remove temp file
}

type Node struct {
	addr        string
	Port        string
	Id          uint
	Successor   string
	Predecessor string
	data        map[string]string
	Fingers     [2]string // [0] id-addr
	SuccId      uint
	PredId      uint
}

var reader *bufio.Reader
var user string
var addr string
var thisNode Node

func main() {

	rpc.Register(new(DHTService))
	//path, _ := os.Getwd()
	//fmt.Println("[PEER] Working directory :" + path)

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "port")
		os.Exit(1)
	}

	addr = "127.0.0.1"
	port := os.Args[1] // Peer port
	idinit := hash(addr + ":" + port)
	sucpredinit := addr + ":" + port
	fingerinit := fmt.Sprint(idinit) + "-" + sucpredinit

	//handle peer requests
	portInt, _ := strconv.Atoi(port)
	go handlePeerRequests(fmt.Sprint(portInt + 10))

	// Init
	thisNode = Node{
		addr:        addr,
		Port:        port,
		Id:          idinit,
		Successor:   sucpredinit,
		Predecessor: sucpredinit,
		data:        map[string]string{},
		Fingers:     [2]string{fingerinit, fingerinit},
		SuccId:      0,
		PredId:      0,
	}

	// Handle Client Requests
	go handleClientRequest()

	var line string
	reader = bufio.NewReader(os.Stdin)
	fmt.Println("\n\n###   Welcome to DStoreLand   ###\n\n1)Enter the peer address to connect\n2)Enter the key to find its successor\n3)Enter the filename to take its hash\n4)Display my-id, succ-id and pred-id\n5)Display the stored file names and their keys\n6)Display the finger table\n7)Exit.\n\n")
	for {
		fmt.Print("\nPlease select an option:")

		line, _ = reader.ReadString('\n')
		option := strings.TrimRight(line, "\r\n")

		handleRequest(option)
	}
}

func handleRequest(option string) {
	var dht DHTService
	switch option {
	case "1": // Enter the peer address to connect
		fmt.Print("Please enter the peer adress to join:")
		line, _ := reader.ReadString('\n')
		addr := strings.TrimRight(line, "\r\n")
		dht.Join(addr)
		break
	case "2": // Enter the key Find its successor
		fmt.Print("Key:")
		line, _ := reader.ReadString('\n')
		key := strings.TrimRight(line, "\r\n")
		fmt.Println(key)
		break
	case "3": // Enter the filename to take its hash
		fmt.Print("Filename:")
		line, _ := reader.ReadString('\n')
		file := strings.TrimRight(line, "\r\n")
		fmt.Println(hash(file))
		break
	case "4": // Display my-id, succ-id, pred-id
		fmt.Printf("(my-id, succ-id, pred-id) -> (%s, %s, %s)\n", fmt.Sprint(thisNode.ID()), fmt.Sprint(thisNode.GetSuccId()), fmt.Sprint(thisNode.GetPredId()))
		break
	case "5": // Display the stored file names and their keys
		for k, v := range thisNode.Data() {
			fmt.Printf("(key, file) -> (%s, %s)", k, v)
		}
		break
	case "6": // Display the finger table
		fmt.Println(thisNode.FingerTable())
		break
	case "7": // Exit
		// While exiting, do the jobs
		os.Exit(0)
		break
	default:
		fmt.Println("default")
		break
	}

}

func handleClientRequest() {

}

func connectDHT() {

}

/* Node getters & setters */
func (node *Node) SetId(id uint) {
	node.Id = id
}
func (node *Node) setAddress(address string) {
	node.addr = address
}

func (node *Node) setSuccessor(succAddress string) {
	node.Successor = succAddress
}

func (node *Node) setPredecessor(predAddress string) {
	node.Predecessor = predAddress
}

func (node Node) Address() string {
	return node.addr
}
func (node Node) ID() uint {
	return node.Id
}
func (node Node) GetSuccId() uint {
	return node.SuccId
}
func (node Node) GetPredId() uint {
	return node.PredId
}
func (node Node) FingerTable() [2]string {
	return node.Fingers
}
func (node Node) Data() map[string]string {
	return node.data
}

func hash(s string) uint {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint(h.Sum32())
}

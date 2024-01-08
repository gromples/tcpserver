// tcpserver/tcpserver.go
package tcpserver

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
)

const MaxClients = 10

const(
	TypeClient = "1"
	TypeWorker = "2"
)

const (
	ErrorCode1001        = "1001"
	ErrorDescription1001 = "Number of connections to broker exceeded"

	ErrorCode1002        = "1002"
	ErrorDescription1002 = "Invalid credentials"

	ErrorCode1003        = "1003"
	ErrorDescription1003 = "Unable to send:"

	// Add more error code and description constants as needed
)

// Client represents a connected client
type Client struct {
	Connection net.Conn
	ServiceIdentifier string
	Type string
	ServiceName string
	MaxParallelism     int
	CurrentJobs int
}

var (
	clients         = make(map[string]Client)
	clientsLock     sync.Mutex
	messageChan     = make(chan map[string]interface{})
	disconnectReq   = make(chan string)
	sendToClient    = make(chan struct {
		ClientName string
		Message    map[string]interface{}
	})
	brokerMessageReceiver chan<- map[string]interface{} // Broker Channel to receive messages from the client
	brokerName        string
	clientNumber    = 1 // Number of client
	clientCount     = 1 // Number of clients
	clientCountMutex sync.Mutex
	clientNumberMutex sync.Mutex
	availableWorkers = make(map[string][]Client)
	availableWorkersLock sync.Mutex
)

type ByCapacity []Client

func (s ByCapacity) Len() int {
	return len(s)
}

func (s ByCapacity) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByCapacity) Less(i, j int) bool {
	// Sort by descending order of available capacity
	capacityI := s[i].MaxParallelism - s[i].CurrentJobs
	capacityJ := s[j].MaxParallelism - s[j].CurrentJobs
	return capacityI > capacityJ
}

// SetMessageReceiver sets the message receiving channel
func SetBrokerMessageReceiver(receiver chan<- map[string]interface{}) {
	brokerMessageReceiver = receiver
}

func init() {
    //fmt.Println("\nTCP SERVER V1.0.0")
}

func deleteWorker(data map[string][]Client, key string, valueToDelete string) {
	availableWorkersLock.Lock()
	defer availableWorkersLock.Unlock();
	if slice, ok := data[key]; ok {
		for i, v := range slice {
			if v.ServiceIdentifier == valueToDelete {
				// Remove the element from the slice
				data[key] = append(slice[:i], slice[i+1:]...)

				// If the slice is empty, delete the entire entry from the map
				if len(data[key]) == 0 {
					delete(data, key)
				}

				break // Stop searching once the record is deleted
			}
		}
	}
}

func FindAvailableWorker(workerType string) (bool,string) {
	availableWorkersLock.Lock()
	defer availableWorkersLock.Unlock();
	////fmt.Printf("\nLook for worker %s.\n", workerType)
	
	if workers, ok := availableWorkers[workerType]; ok {
		sort.Sort(ByCapacity(workers))
		////fmt.Printf("Slice of workers %v.\n",slice)
		if len(workers) > 0 {
			//fmt.Printf("Client with most capacity: %+v\n", workers[0])
			workers[0].CurrentJobs++ //Increment the number of jobs as we are going to send it work
			//fmt.Printf("Client capacity after increment: %+v\n", workers[0])
			return true, workers[0].ServiceIdentifier
		}
	}
	//fmt.Printf("No worker found.\n")
	return false, ""
}

func handleConnection(conn net.Conn, serviceName string,clientType string,maxRequests int) {
	defer conn.Close()

	clientNumberMutex.Lock()
	serviceID := serviceName + strconv.Itoa(clientNumber)
	//fmt.Printf("Client ServiceIdentifier:%s NewName %s ClientNumber:%d.\n", serviceName, serviceID, clientNumber)
	clientNumber++
	clientNumberMutex.Unlock()

	client := Client{Connection: conn, 
		             ServiceIdentifier: serviceID, 
					 Type: clientType, 
					 ServiceName:serviceName,
					 MaxParallelism:maxRequests,
					 CurrentJobs:0}

	// Add the client to the clients map
	clientsLock.Lock()
	clients[serviceID] = client
	clientsLock.Unlock()

	// Add the client to the available workers map
	if clientType==TypeWorker{
		availableWorkersLock.Lock()
		availableWorkers[serviceName] = append(availableWorkers[serviceName],client)
		availableWorkersLock.Unlock()
	}

	fmt.Printf("%s connected to broker %s.\n", serviceID, brokerName)
	//fmt.Printf("Worker pool after adding client:%s %v.\n", serviceID,availableWorkers)

	decoder := json.NewDecoder(conn)

	for {
		var receivedMessage map[string]interface{}
		err := decoder.Decode(&receivedMessage)
		if err != nil {
			//fmt.Printf("Error decoding JSON from client %s on broker %s: %s\n", serviceID, brokerName, err)
			//lowercaseInput := strings.ToLower(err.Error())
			//if strings.Contains(lowercaseInput, "closed") || strings.Contains(lowercaseInput, "eof") {
				clientsLock.Lock()
				delete(clients, serviceID)
				clientsLock.Unlock()
				clientCountMutex.Lock()
				clientCount--
				clientCountMutex.Unlock()
				deleteWorker(availableWorkers, serviceName, serviceID);
				fmt.Printf("%s disconnected from broker %s.\n", serviceID, brokerName)
				fmt.Printf("       ERROR:%s.\n", err.Error())
				return
			//}
			//break
		}

		receivedMessage["SRC"] = serviceID
		// Process the received message as needed
		//fmt.Printf("Received message from client %s on broker %s: %+v\n", serviceID, brokerName, receivedMessage)

		if clientType==TypeWorker{ //add it back to the workers pool
			availableWorkersLock.Lock()
			////fmt.Printf("\n\n\nFinding workers for service %s\n",serviceName)
			if workers, ok := availableWorkers[serviceName]; ok {
				////fmt.Printf("Finding worker with service ID %s\n", serviceID)
				for index := range workers {
					// Check if the Name field matches the targetName
					////fmt.Printf("Range over workers %s(%v):%s(%v)\n", worker.ServiceIdentifier,[]byte(worker.ServiceIdentifier),serviceID,[]byte(serviceID))

					if availableWorkers[serviceName][index].ServiceIdentifier == serviceID {
						//fmt.Println("Worker found. Current jobs:",availableWorkers[serviceName][index].CurrentJobs)
						if availableWorkers[serviceName][index].CurrentJobs > 0{
							//fmt.Printf("Before Decrement current jobs %v\n",availableWorkers[serviceName][index])
							availableWorkers[serviceName][index].CurrentJobs--
							//fmt.Printf("After Decrement current jobs %v\n",availableWorkers[serviceName][index])
							break
						}
					}
				}
			}

			availableWorkersLock.Unlock()
			//fmt.Printf("Worker pool after receiving from:%s %v.\n",serviceID, availableWorkers)
		}

		// Send the received message to the main program
		if brokerMessageReceiver != nil {
			//If you add to a channel in a select and it cannot be done - the other case (default) will be done
			select {
				case brokerMessageReceiver <- receivedMessage:
				default:
				// Handle the case when maxClients is reached
				responeMessage := make(map[string]interface{})
				responeMessage["ERROR_CODE"] = ErrorCode1003
				responeMessage["ERROR_DESCRIPTION"] = ErrorDescription1003 + "Broker for processing - Channel is full"
				//Encode the response and send it to the origin
				encoder := json.NewEncoder(conn)
				err := encoder.Encode(responeMessage)
				//fmt.Printf("Receive Channel Full\n")
				if err != nil {
					//fmt.Printf("Channel is full\n")
					// Optionally handle disconnection or retry logic
				}
			}
		}
	}
}

// SendMessageToClient sends a message to a specific client by serviceName
func SendMessageToClient(clientName string, message map[string]interface{}) (bool, error) {
	clientsLock.Lock()
	client, exists := clients[clientName]
	clientsLock.Unlock()

	if exists {
		//fmt.Printf("Client type found")
		encoder := json.NewEncoder(client.Connection)
		err := encoder.Encode(message)
		if err != nil {
			//fmt.Printf("Error sending message to client %s on broker %s: %s\n", clientName, brokerName, err)
			return false, fmt.Errorf("TCP error (%s) on send to %s\n",err, clientName)
			// Optionally handle disconnection or retry logic
		}else{
			//fmt.Printf("Sending message to client %s on broker %s\n", clientName, brokerName)
		}
	} else {
		//fmt.Printf("Client %s not found on broker %s.------%v\n", clientName, brokerName, clients)
		return false, fmt.Errorf("Destination %s not found", clientName)
		// Optionally handle client not found logic
	}
	return true, nil
}

// StartServer starts the TCP broker on the specified port
func StartServer(port, id string) {
	brokerName = id // Set the serverID
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		//fmt.Printf("Error starting broker %s on port %s: %s\n", brokerName, port, err)
		return
	}
	defer listener.Close()

	fmt.Printf("Broker %s started. Listening on port:%s\n", brokerName, port)

	go func() {
		for {
			select {
			case msg := <-messageChan:
				// Handle messages from clients
				// You can process the messages or broadcast them to all clients, etc.
				if brokerMessageReceiver != nil {
					brokerMessageReceiver <- msg
				}
			case disconnect := <-disconnectReq:
				// Handle client disconnection
				clientsLock.Lock()
				delete(clients, disconnect)
				clientsLock.Unlock()
				//fmt.Printf("Client %s disconnected from broker %s.\n", disconnect, brokerName)
			case send := <-sendToClient:
				// Send a message to a specific client
				SendMessageToClient(send.ClientName, send.Message)
			}
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			//fmt.Printf("Error accepting connection on port %s: %s\n", port, err)
			continue
		}

		// Get the client serviceName from the connection
		var decodedData map[string]interface{}
		err = json.NewDecoder(conn).Decode(&decodedData)
		if err != nil {
			//fmt.Printf("Error getting client serviceName on port %s: %s\n", port, err)
			conn.Close()
			continue
		}

		//fmt.Println("Data received:",decodedData)
		clientName := "(Not Found)"
		if value, ok := decodedData["ServiceName"]; ok {
			// If the key exists, perform a type assertion to convert the value to a string
			if strValue, isString := value.(string); isString {
				// Type assertion successful, assign the value to clientName
				clientName = strValue
			} else {
				//fmt.Println("Value associated with 'serviceName' key is not a string")
				continue
			}
		} else {
			//fmt.Println("Key 'serviceName' not found in the map")
			continue
		}

		connectType := TypeClient
		if value, ok := decodedData["Type"]; ok {
			// If the key exists, perform a type assertion to convert the value to a string
			if strValue, isString := value.(string); isString {
				// Type assertion successful, assign the value to clientName
				connectType = strValue
			} else {
				//fmt.Println("Value associated with 'serviceName' key is not a string")
				continue
			}
		} else {
			//fmt.Println("Key 'serviceName' not found in the map")
			continue
		}

		maxRequests := 1
		if value, ok := decodedData["MaxParallelism"]; ok {
			// If the key exists, perform a type assertion to convert the value to a string
			if strValue, isString := value.(string); isString {
				// Type assertion successful, assign the value to clientName
				num,err := strconv.Atoi(strValue)
				if err == nil {
					maxRequests = num
				}
			} 
		} 

		//fmt.Printf("ClientCount %d MaxClients:%d.\n", clientCount, MaxClients)
		// Check the condition before starting a new goroutine
		if clientCount < MaxClients {
			clientCountMutex.Lock()
			clientCount++
			clientCountMutex.Unlock()
			go handleConnection(conn, clientName, connectType,maxRequests)
		} else {
			// Handle the case when maxClients is reached
			responeMessage := make(map[string]interface{})
			responeMessage["ERROR_CODE"] = ErrorCode1001
			responeMessage["ERROR_DESCRIPTION"] = ErrorDescription1001
			encoder := json.NewEncoder(conn)
			err := encoder.Encode(responeMessage)
			//fmt.Printf("Max Clients:Send Response\n")
			if err != nil {
				//fmt.Printf("Error sending message to client %s: %s\n", clientName, err)
				// Optionally handle disconnection or retry logic
			}
			conn.Close()
		}
	}
}

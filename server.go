package postmaster

import(
	"code.google.com/p/go.net/websocket"
	"github.com/nu7hatch/gouuid"
	"errors"
	"encoding/json"
	"io"
	"sync"
)

///////////////////////////////////////////////////////////////////////////////////////
//
//	Custom Types
//
///////////////////////////////////////////////////////////////////////////////////////

type VerifyFunction func(conn websocket.Config)(bool) // Read-only access
type PublishIntercept func(id ConnectionID, msg PublishMsg)(bool)
type ConnectionID string

type RPCHandler func(ConnectionID,string, string, ...interface{}) (interface{}, *RPCError)

type subscriptionMap struct{
	data map[string] (map[ConnectionID]bool) //Allows concurrent access
	lock *sync.RWMutex
}

func (subMap *subscriptionMap) Find(uri string)([]ConnectionID,bool){
	subMap.lock.RLock()
	idMap,ok := subMap.data[uri]
	subMap.lock.RUnlock()
	
	//Convert map to slice of keys
	var keys []ConnectionID
	for key,_ := range idMap{
		keys = append(keys,key)
	}
	
	return keys,ok
}

func (subMap *subscriptionMap) Add(uri string, id ConnectionID){
	subMap.lock.Lock()
	idMap,ok := subMap.data[uri]
	
	if !ok{
		//Create new map
		newMap := make(map[ConnectionID]bool)
		newMap[id] = true
		subMap.data[uri] = newMap
		
	}else{
		idMap[id] = true
	}
	
	subMap.lock.Unlock()
}

func (subMap *subscriptionMap) Remove(uri string, id ConnectionID){
	subMap.lock.Lock()
	delete(subMap.data[uri],id)
	subMap.lock.Unlock()
}

func newSubscriptionMap()(*subscriptionMap){
	s := new(subscriptionMap)
	s.lock = new(sync.RWMutex)
	s.data = make(map[string] (map[ConnectionID]bool) )
	return s
}

type Connection struct{
	out chan string
	Username string
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Server
//
///////////////////////////////////////////////////////////////////////////////////////

//Represents data storage per instance 
type Server struct{
	localID ConnectionID
	
	//Data storage
	connections map[ConnectionID] Connection // Channel to send on connection TODO Make safe for concurrent access (RWMutex)
	subscriptions *subscriptionMap // Maps subscription URI to connectionID
	rpcHooks map[string] RPCHandler
	
	//Callback functions (Used for external config)
	VerifyID VerifyFunction //Paramater used to filter connections (optional)
	MessageToPublish PublishIntercept

}

func NewServer()*Server{
	return &Server{
		localID: "server", // TODO: Make this something more useful
		connections: make(map[ConnectionID]Connection),
		subscriptions: newSubscriptionMap(),
		rpcHooks: make(map[string]RPCHandler),
		
		VerifyID: nil, //Optional method to filter connections
		MessageToPublish: nil, //Optional method to intercept events before published
	}
}

// Starting point of websocket connection
//	*	Verify identity
//	*	Register send/recieve channel
//	* 	Manage send/recieve for duration of connection
func (t *Server) HandleWebsocket(conn *websocket.Conn) {
	defer conn.Close() //Close connection at end of this function
	
	//Verify ID
	if err := t.verifyConnection(conn); err != nil{
		log.Error("postmaster: error with verification %s",err)
		return
	}
	
	var registeredID ConnectionID
	
	//Register 
	if id,sendChan,err := t.registerConnection(conn); err != nil{
		log.Error("postmaster: error registering connection %s",err)
		return 
	}else{
		registeredID = id
		
		//Setup goroutine to send all message on chan
		go func() {
			for msg := range sendChan {
				log.Trace("postmaster: sending message: %s", msg)
				err := websocket.Message.Send(conn, msg)
				if err != nil {
					log.Error("postmaster: error sending message: %s", err)
				}
			}
		}()
		
	}
		
	//Setup message recieving (Blocking for life of connection)
	t.recieveOnConn(registeredID,conn)
	
	//Unregister connection
	delete(t.connections,registeredID)
}

// Uses the VerifyId function paramater to determine if this is valid connection (Default does nothing)
func (t *Server) verifyConnection(conn *websocket.Conn)(error){

	//Verify identiy
	if t.VerifyID != nil && !t.VerifyID(*conn.Config()){
		return  errors.New("failed verification")// Drop connection
	}else{
		return nil
	}
}

//Returns registered id or error
func (t *Server) registerConnection(conn *websocket.Conn)(ConnectionID,chan string, error){	
	//Create uuid (randomly)
	tid, err := uuid.NewV4()
	if err != nil {
		return "",nil,errors.New("error creating uuid:"+err.Error())
	}
	
	id := ConnectionID(tid.String())
	
	//Create Welcome
	msg := WelcomeMsg{SessionId:string(id)}
	
	arr, jsonErr := msg.MarshalJSON()
	if jsonErr != nil {
		return "",nil,errors.New("error encoding welcome message:"+jsonErr.Error())
	}
	
	//Send welcome
	log.Debug("sending welcome message: %s", arr)
	
	if err := websocket.Message.Send(conn, string(arr)); err != nil {
		return "",nil,errors.New("error sending welcome message, aborting connection:"+ err.Error())
	}
	
	log.Info("client connected: %s", id)
	
	//Make channel to send on
	sendChan := make(chan string, ALLOWED_BACKLOG)
	
	//Find username for conn TODO make this application specific
	username := conn.Config().Header.Get("musicbox-username")
	
	//Register channel with server	
	newConn := Connection{out:sendChan,Username:username}
	t.connections[id] = newConn
	
	return  id,sendChan,nil //Sucessfully registered
}

//Recieves on channel for life of connection
func (t *Server) recieveOnConn(id ConnectionID, conn *websocket.Conn){	
	Connection_Loop:
	for {
		//Recieve message
		var rec string
		err := websocket.Message.Receive(conn, &rec)
		if err != nil {
			//Don't error on normal socket close
			if err != io.EOF {
				log.Error("postmaster: error receiving message, aborting connection: %s", err)
			}
			break Connection_Loop
		}
		log.Trace("postmaster: message received: %s", rec)
		
		//Process message
		data := []byte(rec)

		switch typ := parseType(rec); typ {
		case CALL:
			var msg CallMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Error("postmaster: error unmarshalling call message: %s", err)
				continue Connection_Loop
			}
			t.handleCall(id, msg)
		case SUBSCRIBE:
			var msg SubscribeMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Error("postmaster: error unmarshalling subscribe message: %s", err)
				continue Connection_Loop
			}
			t.handleSubscribe(id, msg)
		case UNSUBSCRIBE:
			var msg UnsubscribeMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Error("postmaster: error unmarshalling unsubscribe message: %s", err)
				continue Connection_Loop
			}
			t.handleUnsubscribe(id, msg)
		case PUBLISH:
			var msg PublishMsg
			err := json.Unmarshal(data, &msg)
			if err != nil {
				log.Error("postmaster: error unmarshalling publish message: %s", err)
				continue Connection_Loop
			}
			t.handlePublish(id, msg)
		case WELCOME, CALLRESULT, CALLERROR, EVENT, PREFIX:
			log.Error("postmaster: server -> client message received, ignored: %d", typ)
		default:
			log.Error("postmaster: invalid message format, message dropped: %s", data)
		}
		
	}
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	WAMP Message Handling
//
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handlePublish(id ConnectionID, msg PublishMsg){
	log.Trace("postmaster: handling publish message")
		
	subscribers,_ := t.subscriptions.Find(msg.TopicURI) //Doesn't matter if no one listening on this instance; possibly on other instances
	
	event := &EventMsg{
		TopicURI: msg.TopicURI,
		Event: msg.Event,
	}
	
	//Give server option to intercept and/or kill event
	if t.MessageToPublish != nil && !t.MessageToPublish(id,msg){
		log.Debug("postmaster: event vetoed by server: %s",msg.Event)
		return
	}
	
	//Format json and distribute
	if jsonEvent, err := event.MarshalJSON(); err == nil{
		//TODO Pass to other instances
		
		//Loop over all connections for subscription
		for _,connID := range subscribers{
			//Look up connection for this ID
			if conn,ok := t.connections[connID]; ok && connID != id{
				conn.out <- string(jsonEvent)
			}else if !ok{
				//Remove subscription of dropped connection
				t.subscriptions.Remove(msg.TopicURI,connID)
			}
		}
	}else{
		log.Error("postmaster: error creating event message: %s", err)
		return
	}
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleCall(id ConnectionID, msg CallMsg){
	log.Trace("postmaster: handling call message")

	var out []byte

	//Check if function exists
	if f, ok := t.rpcHooks[msg.ProcURI]; ok && f != nil {
		username := t.connections[id].Username
		
		// Perform function
		res, err := f(id,username, msg.ProcURI, msg.CallArgs...)
	
		if err == nil{
			//Formulate response
			callResult := &CallResultMsg{
				CallID: msg.CallID,
				Result: res,
			}
			out,_ = callResult.MarshalJSON()
			
		} else {
			//Handle error
			callError := &CallErrorMsg{
				CallID: msg.CallID,
				ErrorURI: err.URI,
				ErrorDesc: err.Description,
				ErrorDetails: err.Details,
			}
			out,_ = callError.MarshalJSON()
		}
	} else {
		log.Warn("postmaster: RPC call not registered: %s", msg.ProcURI)
		callError := &CallErrorMsg{
			CallID: msg.CallID,
			ErrorURI: "error:notimplemented",
			ErrorDesc: "RPC call '%s' not implemented",
			ErrorDetails: msg.ProcURI,
		}
		out,_ = callError.MarshalJSON()		
	}

	if conn, ok := t.connections[id]; ok {
		conn.out <- string(out)
	}
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleSubscribe(id ConnectionID, msg SubscribeMsg){
	username := t.connections[id].Username
	t.subscriptions.Add(username+":"+msg.TopicURI,id) //Add to subscriptions
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleUnsubscribe(id ConnectionID, msg UnsubscribeMsg){
	username := t.connections[id].Username
	t.subscriptions.Remove(username+":"+msg.TopicURI,id) //Remove from subscriptions
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Server Functionality
//
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) RegisterRPC(uri string, f RPCHandler) {
	if f != nil {
		t.rpcHooks[uri] = f
	}
}

func (t *Server) UnregisterRPC(uri string) {
	delete(t.rpcHooks, uri)
}

func (t *Server) SendEvent(topic string, event interface{}) {
	t.handlePublish(t.localID, PublishMsg{
		TopicURI: topic,
		Event:    event,
	})
}

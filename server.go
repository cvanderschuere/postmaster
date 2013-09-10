package postmaster

import(
	"code.google.com/p/go.net/websocket"
	"github.com/nu7hatch/gouuid"
	"errors"
	"encoding/json"
	"io"
	"sync"
)

//Constants

const WAMP_BASE_URL = "http://api.wamp.ws/"
const WAMP_PROCEDURE_URL = WAMP_BASE_URL+"procedure#"

///////////////////////////////////////////////////////////////////////////////////////
//
//	Custom Types
//
///////////////////////////////////////////////////////////////////////////////////////

type PublishIntercept func(id *Connection, msg PublishMsg)(bool)

type RPCHandler func(*Connection, string, ...interface{}) (interface{}, *RPCError)

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

type ConnectionID string

type Connection struct{
	out chan string
	isAuth bool //Has this client been authenticated (only allow authreq/auth rpc call if not)
	id ConnectionID //Used internally
	
	Username string
	P *Permissions //Permission for this client
}

type Permissions struct{
	RPC map[string] RPCPermission //maps uri to RPCPermission
	PubSub map[string] PubSubPermission //maps uri to PubSubPermission
}

type RPCPermission bool

type PubSubPermission struct{
	canPublish bool
	canSubscribe bool
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Server
//
///////////////////////////////////////////////////////////////////////////////////////

//Represents data storage per instance 
type Server struct{
	localID string //TODO : Don't use this
	
	//Data storage
	connections map[ConnectionID] *Connection // Channel to send on connection TODO Make safe for concurrent access (RWMutex)
	subscriptions *subscriptionMap // Maps subscription URI to connectionID
	rpcHooks map[string] RPCHandler
	
	//
	//Challenge Response Authentication Callbacks
	//
	
	//Get the authentication secret for an authentication key, i.e. the user password for the user name. Return "" when the authentication key does not exist.
	GetAuthSecret	func(authKey string)(string) // Required
	
    //Get the permissions the session is granted when the authentication succeeds for the given key / extra information.
	GetAuthPermissions func(authKey string,authExtra map[string]interface{})(Permissions) // Required
	
	//Fired when client authentication was successful.
	OnAuthenticated func(authKey string, permission Permissions) // Optional

	//Message interept
	MessageToPublish PublishIntercept // Optional

}

func NewServer()*Server{
	return &Server{
		localID: "server", // TODO: Make this something more useful
		connections: make(map[ConnectionID]*Connection),
		subscriptions: newSubscriptionMap(),
		rpcHooks: make(map[string]RPCHandler),
				
		//Callbacks all nil (Note some are required)
	}
}

// Starting point of websocket connection
//	*	Verify identity
//	*	Register send/recieve channel
//	* 	Manage send/recieve for duration of connection
func (t *Server) HandleWebsocket(conn *websocket.Conn) {
	defer conn.Close() //Close connection at end of this function
	
	//Register Connection
	c,err := t.registerConnection(conn)
	if err != nil{
		log.Error("postmaster: error registering connection: %s", err)
		return
	}
		
	//Setup goroutine to send all message on chan
	go func() {
		for msg := range c.out {
			log.Trace("postmaster: sending message: %s", msg)
			err := websocket.Message.Send(conn, msg)
			if err != nil {
				log.Error("postmaster: error sending message: %s", err)
			}
		}
	}()	
		
	//Setup message recieving (Blocking for life of connection)
	t.recieveOnConn(c,conn)
	
	//Unregister connection
	delete(t.connections,c.id)
}

//Returns registered id or error
func (t *Server) registerConnection(conn *websocket.Conn)(*Connection,error){	
	//Create uuid (randomly)
	tid, err := uuid.NewV4()
	if err != nil {
		return nil,errors.New("error creating uuid:"+err.Error())
	}
	
	cid := ConnectionID(tid.String())
	
	//Create Welcome
	msg := WelcomeMsg{SessionId:string(cid)}
	
	arr, jsonErr := msg.MarshalJSON()
	if jsonErr != nil {
		return nil,errors.New("error encoding welcome message:"+jsonErr.Error())
	}
	
	//Send welcome
	log.Debug("sending welcome message: %s", arr)
	
	if err := websocket.Message.Send(conn, string(arr)); err != nil {
		return nil,errors.New("error sending welcome message, aborting connection:"+ err.Error())
	}
	
	//Create Connection
	sendChan := make(chan string, ALLOWED_BACKLOG) //Channel to send to connection
	
	//Register channel with server	
	newConn := &Connection{out:sendChan,id:cid,isAuth:false,Username:"",P:nil} //Un authed user
	t.connections[cid] = newConn
	
	log.Info("client connected: %s", cid)
	
	return  newConn,nil //Sucessfully registered
}

//Recieves on channel for life of connection
func (t *Server) recieveOnConn(conn *Connection, ws *websocket.Conn){	
	Connection_Loop:
	for {
		//Recieve message
		var rec string
		err := websocket.Message.Receive(ws, &rec)
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
			t.handleCall(conn, msg)
		case SUBSCRIBE:
			if conn.isAuth{
				var msg SubscribeMsg
				err := json.Unmarshal(data, &msg)
				if err != nil {
					log.Error("postmaster: error unmarshalling subscribe message: %s", err)
					continue Connection_Loop
				}
				t.handleSubscribe(conn, msg)
			}
		case UNSUBSCRIBE:
			if conn.isAuth{
				var msg UnsubscribeMsg
				err := json.Unmarshal(data, &msg)
				if err != nil {
					log.Error("postmaster: error unmarshalling unsubscribe message: %s", err)
					continue Connection_Loop
				}
				t.handleUnsubscribe(conn, msg)
			}
		case PUBLISH:
			if conn.isAuth{
				var msg PublishMsg
				err := json.Unmarshal(data, &msg)
				if err != nil {
					log.Error("postmaster: error unmarshalling publish message: %s", err)
					continue Connection_Loop
				}
				t.handlePublish(conn, msg)
			}
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

func (t *Server) handlePublish(conn *Connection, msg PublishMsg){
	log.Trace("postmaster: handling publish message")
	
	
	//Make sure this connection can publish on this uri
	if r := conn.P.PubSub[msg.TopicURI];r.canPublish == false{
		log.Error("postmaster: Connection tried to publish to incorrect uri")
		return
	}
		
	subscribers,_ := t.subscriptions.Find(msg.TopicURI) //Doesn't matter if no one listening on this instance; possibly on other instances
	
	event := &EventMsg{
		TopicURI: msg.TopicURI,
		Event: msg.Event,
	}
	
	//Give server option to intercept and/or kill event
	if t.MessageToPublish != nil && !t.MessageToPublish(conn,msg){
		log.Debug("postmaster: event vetoed by server: %s",msg.Event)
		return
	}
	
	//Format json and distribute
	if jsonEvent, err := event.MarshalJSON(); err == nil{
		//TODO Pass to other instances
		
		//Loop over all connections for subscription
		for _,connID := range subscribers{
			//Look up connection for this ID
			if subConn,ok := t.connections[connID]; ok && connID != conn.id{
				subConn.out <- string(jsonEvent)
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

func (t *Server) handleCall(conn *Connection, msg CallMsg){
	log.Trace("postmaster: handling call message")
	
	//Make sure this is appropriate call (only authreq/auth when isAuth==false)
	if !conn.isAuth && ((msg.ProcURI != WAMP_PROCEDURE_URL+"authreq") || (msg.ProcURI != WAMP_PROCEDURE_URL+"auth")){
		log.Error("Tried to make rpc call other than authreq/auth on unauthenticated connection")
		return
	}

	var out []byte

	//Check if function exists
	if f, ok := t.rpcHooks[msg.ProcURI]; ok && f != nil {
		
		// Perform function
		res, err := f(conn, msg.ProcURI, msg.CallArgs...)
	
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

	if returnConn, ok := t.connections[conn.id]; ok {
		returnConn.out <- string(out)
	}
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleSubscribe(conn *Connection, msg SubscribeMsg){
	//Make sure this connection can publish on this uri
	if r := conn.P.PubSub[msg.TopicURI];r.canSubscribe == false{
		log.Error("postmaster: Connection tried to subscrive to incorrect uri")
		return
	}
	
	username := t.connections[conn.id].Username
	t.subscriptions.Add(username+":"+msg.TopicURI,conn.id) //Add to subscriptions
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleUnsubscribe(conn *Connection, msg UnsubscribeMsg){
	t.subscriptions.Remove(conn.Username+":"+msg.TopicURI,conn.id) //Remove from subscriptions
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

/*
func (t *Server) SendEvent(topic string, event interface{}) {
	t.handlePublish(t.localID, PublishMsg{
		TopicURI: topic,
		Event:    event,
	})
}
*/

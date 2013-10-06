package postmaster

import(
	"code.google.com/p/go.net/websocket"
	"github.com/nu7hatch/gouuid"
	"errors"
	"encoding/json"
	"io"
)

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
	unauthRPCHooks map[string] RPCHandler
	
	//
	//Challenge Response Authentication Callbacks
	//
	
	//Get the authentication secret for an authentication key, i.e. the user password for the user name. Return "" when the authentication key does not exist.
	GetAuthSecret	func(authKey string)(string,error) // Required
	
    //Get the permissions the session is granted when the authentication succeeds for the given key / extra information.
	GetAuthPermissions func(authKey string,authExtra map[string]interface{})(Permissions,error) // Required
	
	//Fired when client authentication was successful.
	OnAuthenticated func(authKey string,authExtra map[string]interface{}, permission Permissions) // Optional

	//Message interept
	MessageToPublish PublishIntercept // Optional
	
	//Fired when authenticated client disconnections
	OnDisconnect func(authKey string,authExtra map[string]interface{}) //Optional

}

func NewServer()*Server{
	return &Server{
		localID: "server", // TODO: Make this something more useful
		connections: make(map[ConnectionID]*Connection),
		subscriptions: newSubscriptionMap(),
		rpcHooks: make(map[string]RPCHandler),
		unauthRPCHooks: make(map[string]RPCHandler),
				
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
	
	//Call disconnection method
	if t.OnDisconnect != nil{
		t.OnDisconnect(c.pendingAuth.authKey,c.pendingAuth.authExtra)
	}
	
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
	newConn := &Connection{out:sendChan,id:cid,pendingAuth:nil,isAuth:false,Username:"",P:nil} //Un authed user
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
	if r := conn.P.PubSub[msg.TopicURI];r.CanPublish == false{
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
	
	hookMap := t.rpcHooks
	
	//Make sure this is appropriate call (only authreq/auth when isAuth==false)
	if !conn.isAuth {
		switch msg.ProcURI{
		case WAMP_PROCEDURE_URL+"authreq":
			//Get args
			authKey := msg.CallArgs[0].(string)
			var authExtra map[string]interface{}
			var ok bool
			if len(msg.CallArgs)>1 && msg.CallArgs[1] != nil{
				authExtra,ok = msg.CallArgs[1].(map[string]interface{})
				if !ok{
					authExtra = nil
				}
			}else{
				authExtra = nil
			}
			
			res,err := authRequest(t,conn,authKey,authExtra)
			if err == nil{
				callResult := &CallResultMsg{
					CallID: msg.CallID,
					Result: res,
				}
				out,_ := callResult.MarshalJSON()
				if returnConn, ok := t.connections[conn.id]; ok {
					returnConn.out <- string(out)
				}
			}
			return
		case WAMP_PROCEDURE_URL+"auth":
			res,err := auth(t,conn,msg.CallArgs[0].(string))
			if err == nil{
				callResult := &CallResultMsg{
					CallID: msg.CallID,
					Result: res,
				}
				out,_ := callResult.MarshalJSON()
				if returnConn, ok := t.connections[conn.id]; ok {
					returnConn.out <- string(out)
				}
			}
			return
		default:
			hookMap = t.unauthRPCHooks //Use unauthRPC hooks
			
		}
	}

	var out []byte

	//Check if function exists
	if f, ok := hookMap[msg.ProcURI]; ok && f != nil {
		
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
	if r := conn.P.PubSub[msg.TopicURI];r.CanSubscribe == false{
		log.Error("postmaster: Connection tried to subscrive to incorrect uri")
		return
	}
	
	t.subscriptions.Add(msg.TopicURI,conn.id) //Add to subscriptions
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func (t *Server) handleUnsubscribe(conn *Connection, msg UnsubscribeMsg){
	t.subscriptions.Remove(msg.TopicURI,conn.id) //Remove from subscriptions
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

func (t *Server) RegisterUnauthRPC(uri string, f RPCHandler) {
	if f != nil {
		t.unauthRPCHooks[uri] = f
	}
}

func (t *Server) UnregisterUnauthRPC(uri string) {
	delete(t.unauthRPCHooks, uri)
}

//Publish event outside of normal client->client structure
func (t *Server) PublishEvent(uri string,msg interface{}){
	subscribers,_ := t.subscriptions.Find(uri) //Doesn't matter if no one listening on this instance; possibly on other instances
	
	event := &EventMsg{
		TopicURI: uri,
		Event: msg,
	}
	
	//Format json and distribute
	if jsonEvent, err := event.MarshalJSON(); err == nil{
		//TODO Pass to other instances
		
		//Loop over all connections for subscription
		for _,connID := range subscribers{
			//Look up connection for this ID
			if subConn,ok := t.connections[connID]; ok{
				subConn.out <- string(jsonEvent)
			}else if !ok{
				//Remove subscription of dropped connection
				t.subscriptions.Remove(uri,connID)
			}
		}
	}else{
		log.Error("postmaster: error creating event message(PublishEvent()): %s", err)
		return
	}
}

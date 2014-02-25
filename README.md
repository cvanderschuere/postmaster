Postmaster
==========

General purpose wamp messaging server


* Pass messages following WAMP protocol
* Support for server side message intercept
* Support for server events OnAuthenticated and OnDisconnect per connection
* Scalable (Initial work to make multi-instance aware)


##Getting Started

Here is an example server that can be made with Postmaster.

Features of this example:
* Authentication
* Callbacks
* Unauthenticated RPC
* Authenticated RPC

```go
import (
    "postmaster"
    "code.google.com/p/go.net/websocket"
)

func main(){
    server := postmaster.NewServer()

	//Assign auth callbacks
	server.GetAuthSecret = lookupUser
	server.GetAuthPermissions = getUserPremissions
	server.OnAuthenticated = userConnected
	server.OnDisconnect = clientDisconnected

	server.MessageToPublish = interceptMessage

	//Unauth rpc
	server.RegisterUnauthRPC(baseURL+"helloWorld",helloWorld)
	
	//Setup Authenticated RPC Functions
	server.RegisterRPC(baseURL+"helloWorldAuth",helloWorld)

    s := websocket.Server{Handler: postmaster.HandleWebsocket(server), Handshake: nil}
	http.Handle("/", s)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

/*
    TODO: Define all of the callback functions

*/

```

##Callbacks

###Authentication
```go
//Get the authentication secret for an authentication key, i.e. the user password for the user name. Return "" when the authentication key does not exist.
GetAuthSecret	func(authKey string)(string,error) // Required
```

```go
//Get the permissions the session is granted when the authentication succeeds for the given key / extra information.
GetAuthPermissions func(authKey string,authExtra map[string]interface{})(Permissions,error) // Required
```

```go
//Fired when client authentication was successful.
OnAuthenticated func(authKey string,authExtra map[string]interface{}, permission Permissions) // Optional
```

###Server Intercept

```go
//Message interept
MessageToPublish PublishIntercept // Optional
```

```go
//Fired when authenticated client disconnections
OnDisconnect func(authKey string,authExtra map[string]interface{}) //Optional
```

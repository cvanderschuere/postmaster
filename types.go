package postmaster

import(
	"sync"
)

///////////////////////////////////////////////////////////////////////////////////////
//
//	Custom Types
//
///////////////////////////////////////////////////////////////////////////////////////

//
// Connection types
//

type ConnectionID string

type Connection struct{
	out chan string
	pendingAuth *PendingAuth //Set in between authreq & auth
	isAuth bool //Has this client been authenticated (only allow authreq/auth rpc call if not)
	id ConnectionID //Used internally
	
	Username string
	P *Permissions //Permission for this client
}

//
// Auth Types
//

type PendingAuth struct{
	authKey string
	authExtra map[string]interface{}
	sig string
	p Permissions
	ch []byte //Challenge in json form
	
}

type Permissions struct{
	RPC map[string] RPCPermission //maps uri to RPCPermission
	PubSub map[string] PubSubPermission //maps uri to PubSubPermission
}

type RPCPermission bool

type PubSubPermission struct{
	CanPublish bool
	CanSubscribe bool
}

//
// Server Types
//

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

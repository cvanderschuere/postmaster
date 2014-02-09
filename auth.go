package postmaster

import(
	"github.com/nu7hatch/gouuid"
	"errors"
	"time"
	"code.google.com/p/go.crypto/pbkdf2"
	"encoding/base64"
	"crypto/sha256"
	"crypto/hmac"
	"encoding/json"
	"strings"
)

//
//Challenge Response Authentication Methods
//

//RPC endpoint for clients to initiate the authentication handshake.
//returns string -- Authentication challenge. The client will need to create an authentication signature from this.
func authRequest(t *Server, conn *Connection, authKey string, authExtra map[string]interface{})(string,error){
	//Check for states that don't support authreq
	if conn.isAuth{
	 	return "",errors.New("Connection already authenticated")
	}else if conn.pendingAuth != nil{
		return "",errors.New("Authentication request already issues - authentication pending")
	}
		
	//Get authKey TODO: add anynomous auth option
	var secret string
	if t.GetAuthSecret != nil{
		var err error
		secret,err = t.GetAuthSecret(authKey)
		if err != nil{
			return "",err //No matching secret: user probably doesn't exist
		}
	}else{
		log.Error("GetAuthSecret nil: required method")
		panic("Nil required method")
	}
	
	authID,_ := uuid.NewV4()
	
	//Create challenge
	ch := map[string]interface{}{
		"authid": authID,
		"authkey": authKey,
		"timestamp": time.Now().UTC().Format(time.RFC3339), //Might need to remove timezone information
		"sessionid": conn.id,
		"extra": authExtra,
		"permissions":map[string]interface{}{ //Create false (always blank) permissions for autobahn compatability
			"pubsub":[]string{},
			"rpc":[]string{},
		},
	}
	
	//Get Permission of this user
	perms,err := t.GetAuthPermissions(authKey,authExtra)
	if err != nil{
		log.Error("Error getting auth permissions")
	}
	
	//Get signature for this key
	authChallenge,_ := json.Marshal(ch) //Create challenge string
	s := authSignature(authChallenge,secret,authExtra)
	
	pend := &PendingAuth{
		authKey:authKey,
		authExtra:authExtra,
		sig:s,
		p: perms,
		ch:authChallenge,
	}
	
	conn.pendingAuth = pend //Save for later auth rpc call
	
	return string(authChallenge),nil
} 
//RPC endpoint for clients to actually authenticate after requesting authentication and computing a signature from the authentication challenge.
func auth(t *Server, conn *Connection, signature string)(*Permissions,error){
	if conn.isAuth{
	 	return nil,errors.New("Connection already authenticated")
	}else if conn.pendingAuth == nil{
		return nil,errors.New("No pending authentication; call authreq first")
	}
	
	log.Info(signature + "::" + conn.pendingAuth.sig)
	
	//Check signature
	if signature != conn.pendingAuth.sig{
		conn.pendingAuth = nil
		return nil,errors.New("Invalid signature; repeat with authreq")
	}
	
	//
	//Now sucessfully authenticated
	//
	
	conn.isAuth = true
	conn.P = &conn.pendingAuth.p //Set permissions
	conn.Username = strings.ToLower(conn.pendingAuth.authKey) //FIXME probably best to do this outside of postmaster
	
	if t.OnAuthenticated != nil{
		go t.OnAuthenticated(conn.Username, conn.pendingAuth.authExtra, *conn.P) //Signal to server that new conneciton made
	}

	return conn.P,nil;
}


//
// Crypto
//

/*
	Computes a derived cryptographic key from a password according to PBKDF2 http://en.wikipedia.org/wiki/PBKDF2.

	The function will only return a derived key if at least 'salt' is present in the 'extra' dictionary. The complete set of attributesthat can be set in 'extra':

         salt: The salt value to be used.
         iterations: Number of iterations of derivation algorithm to run.
         keylen: Key length to derive.

	returns the derived key or the original secret.
*/
func deriveKey(secret string, extra map[string]interface{})(string){
	//Salt needed to derive key
	if salt,ok := extra["salt"]; ok{
		iter := 10000
		keyLen := 32
		
		//Check for custom values
		if cIter,ok := extra["iterations"]; ok{
			iter = cIter.(int)
		}
		if cKeylen,ok := extra["keylen"];ok{
			keyLen = cKeylen.(int)
		}
		
		dk := pbkdf2.Key([]byte(secret), []byte(salt.(string)), iter, keyLen, sha256.New)
		key := base64.StdEncoding.EncodeToString(dk)
		
		return key
		
	}else{
		//just return secret
		return secret
	}	
}

func authSignature(authChallenge []byte,authSecret string, authExtra map[string]interface{})(string){
	//Derive authsecret
	authSecret = deriveKey(authSecret,authExtra)
	
	//Generate signature
	sHash :=  hmac.New(sha256.New,[]byte(authSecret))
	sHash.Write(authChallenge)
	sig := sHash.Sum(nil)
	
	//Convert to ascii binary
	s := base64.StdEncoding.EncodeToString(sig)
	
	return s
}
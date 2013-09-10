package postmaster

//
//Challenge Response Authentication Methods
//

//RPC endpoint for clients to initiate the authentication handshake.
//returns string -- Authentication challenge. The client will need to create an authentication signature from this.
func authRequest(t *Server, conn *Connection, authKey string, extra map[string]interface{})(string,error){
	
} 
//RPC endpoint for clients to actually authenticate after requesting authentication and computing a signature from the authentication challenge.
func auth(t *Server, conn *Connection, signature string)(error){
	
}
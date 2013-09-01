package postmaster

import(
	"regexp"
	"strconv"
	"encoding/json"
)

///////////////////////////////////////////////////////////////////////////////////////
//
// WAMP constants
//
///////////////////////////////////////////////////////////////////////////////////////

type MessageType int
const (
	WELCOME MessageType = iota
	PREFIX
	CALL
	CALLRESULT
	CALLERROR
	SUBSCRIBE
	UNSUBSCRIBE
	PUBLISH
	EVENT
)

const PROTOCOL_VERSION = 1

var (
	typeReg = regexp.MustCompile("^\\s*\\[\\s*(\\d+)\\s*,")
)

///////////////////////////////////////////////////////////////////////////////////////
//
//	WAMP types
//
///////////////////////////////////////////////////////////////////////////////////////

type WAMPError struct {
	Msg string
}

var (
	ErrInvalidURI          = &WAMPError{"invalid URI"}
	ErrInvalidNumArgs      = &WAMPError{"invalid number of arguments in message"}
	ErrUnsupportedProtocol = &WAMPError{"unsupported protocol"}
)

func (e *WAMPError) Error() string {
	return "wamp: " + e.Msg
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type RPCError struct {
	URI string
	Description string
	Details interface{}
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type WelcomeMsg struct {
	SessionId       string
	ProtocolVersion int
	ServerIdent     string
}

func (msg *WelcomeMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) != 4 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.SessionId, ok = data[1].(string); !ok {
		return &WAMPError{"invalid session ID"}
	}
	if protocolVersion, ok := data[2].(float64); ok {
		msg.ProtocolVersion = int(protocolVersion)
	} else {
		return ErrUnsupportedProtocol
	}
	if msg.ServerIdent, ok = data[3].(string); !ok {
		return &WAMPError{"invalid server identity"}
	}
	return nil
}

func (msg* WelcomeMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(WELCOME, msg.SessionId, PROTOCOL_VERSION, POSTMASTER_SERVER_ID)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type CallMsg struct {
	CallID   string
	ProcURI  string
	CallArgs []interface{}
}

func (msg *CallMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) < 3 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.CallID, ok = data[1].(string); !ok {
		return &WAMPError{"invalid callID"}
	}
	if msg.ProcURI, ok = data[2].(string); !ok {
		return &WAMPError{"invalid procURI"}
	}
	if len(data) > 3 {
		msg.CallArgs = data[3:]
	}
	return nil
}

func (msg* CallMsg) MarshalJSON() ([]byte, error){
	data := []interface{}{CALL, msg.CallID, msg.ProcURI}
	data = append(data,msg.CallArgs...)
	return createWAMPMessage(data...)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type CallResultMsg struct {
	CallID string
	Result interface{}
}

func (msg *CallResultMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) != 3 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.CallID, ok = data[1].(string); !ok {
		return &WAMPError{"invalid callID"}
	}
	msg.Result = data[2]

	return nil
}

func (msg* CallResultMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(CALLRESULT, msg.CallID, msg.Result)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type CallErrorMsg struct {
	CallID       string
	ErrorURI     string
	ErrorDesc    string
	ErrorDetails interface{} 
}

func (msg *CallErrorMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) < 4 || len(data) > 5 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.CallID, ok = data[1].(string); !ok {
		return &WAMPError{"invalid callID"}
	}
	if msg.ErrorURI, ok = data[2].(string); !ok {
		return &WAMPError{"invalid errorURI"}
	}
	if msg.ErrorDesc, ok = data[3].(string); !ok {
		return &WAMPError{"invalid error description"}
	}
	if len(data) == 5 {
		msg.ErrorDetails = data[4]
	}
	return nil
}

func (msg* CallErrorMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(CALLERROR, msg.CallID, msg.ErrorURI, msg.ErrorDesc,msg.ErrorDetails)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type SubscribeMsg struct {
	TopicURI string
}

func (msg *SubscribeMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) != 2 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.TopicURI, ok = data[1].(string); !ok {
		return &WAMPError{"invalid topicURI"}
	}
	return nil
}

func (msg* SubscribeMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(SUBSCRIBE, msg.TopicURI)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type UnsubscribeMsg struct {
	TopicURI string
}

func (msg *UnsubscribeMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) != 2 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.TopicURI, ok = data[1].(string); !ok {
		return &WAMPError{"invalid topicURI"}
	}
	return nil
}

func (msg* UnsubscribeMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(UNSUBSCRIBE, msg.TopicURI)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type PublishMsg struct {
	TopicURI     string
	Event        interface{}
	ExcludeMe    bool 
	ExcludeList  []string 
	EligibleList []string 
}

func (msg *PublishMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) < 3 || len(data) > 5 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.TopicURI, ok = data[1].(string); !ok {
		return &WAMPError{"invalid topicURI"}
	}
	msg.Event = data[2]
	if len(data) > 3 {
		if msg.ExcludeMe, ok = data[3].(bool); !ok {
			var arr []interface{}
			if arr, ok = data[3].([]interface{}); !ok && data[3] != nil {
				return &WAMPError{"invalid exclude argument"}
			}
			for _, v := range arr {
				if val, ok := v.(string); !ok {
					return &WAMPError{"invalid exclude list"}
				} else {
					msg.ExcludeList = append(msg.ExcludeList, val)
				}
			}
			if len(data) == 5 {
				if arr, ok = data[4].([]interface{}); !ok && data[3] != nil {
					return &WAMPError{"invalid eligable list"}
				}
				for _, v := range arr {
					if val, ok := v.(string); !ok {
						return &WAMPError{"invalid eligable list"}
					} else {
						msg.EligibleList = append(msg.EligibleList, val)
					}
				}
			}
		}
	}
	return nil
}

func (msg* PublishMsg) MarshalJSON()([]byte, error){
	return createWAMPMessage(PUBLISH, msg.TopicURI, msg.Event,msg.ExcludeMe)
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

type EventMsg struct {
	TopicURI string
	Event    interface{}
}

func (msg *EventMsg) UnmarshalJSON(jsonData []byte) error {
	var data []interface{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return err
	}
	if len(data) != 3 {
		return ErrInvalidNumArgs
	}
	var ok bool
	if msg.TopicURI, ok = data[1].(string); !ok {
		return &WAMPError{"invalid topicURI"}
	}
	msg.Event = data[2]

	return nil
}

func (msg* EventMsg) MarshalJSON() ([]byte, error){
	return createWAMPMessage(EVENT, msg.TopicURI, msg.Event)
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Utility Functions
//
///////////////////////////////////////////////////////////////////////////////////////

func parseType(msg string) MessageType {
	match := typeReg.FindStringSubmatch(msg)
	if match == nil {
		return -1
	}
	i, _ := strconv.Atoi(match[1])
	return MessageType(i)
}

// createWAMPMessage returns a JSON encoded list from all the arguments passed to it
func createWAMPMessage(args ...interface{}) ([]byte,error) {
	return json.Marshal(args)
}


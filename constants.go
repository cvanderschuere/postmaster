package postmaster

import(
	"github.com/jcelliott/lumber"
)
var (
	log lumber.Logger = lumber.NewConsoleLogger(lumber.INFO)
)

const (
	POSTMASTER_VERSION      = "0.2.0"
	POSTMASTER_SERVER_ID = "postmaster-" + POSTMASTER_VERSION
)

const ALLOWED_BACKLOG = 6

//Auth: wamp cra
const WAMP_BASE_URL = "http://api.wamp.ws/"
const WAMP_PROCEDURE_URL = WAMP_BASE_URL+"procedure#"

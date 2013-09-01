package postmaster

import(
	"github.com/jcelliott/lumber"
)
var (
	log lumber.Logger = lumber.NewConsoleLogger(lumber.TRACE)
)

const (
	POSTMASTER_VERSION      = "0.2.0"
	POSTMASTER_SERVER_ID = "postmaster-" + POSTMASTER_VERSION
)

const ALLOWED_BACKLOG = 6
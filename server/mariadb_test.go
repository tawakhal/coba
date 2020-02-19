package server

import (
	"encoding/json"
	"fmt"
	"testing"

	ul "git.bluebird.id/bluebird/util/log"
)

const (
	url      = "127.0.0.1:3306"
	schema   = "area"
	user     = "root"
	password = "root"
)

func TestReadLocationByAreaName(t *testing.T) {

	t.Fail()

	Logger = ul.StdLogger()

	Logger.Log("[INFO]", "Server Started...")

	// initial db
	rw := NewDBReadWriter(url, schema, user, password)

	dt, err := rw.ReadLocationByAreaName("Jakarta")
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return
	}

	Logger.Log("[INFO]", fmt.Sprintf("%+v", dt))

	jsonData, err := json.Marshal(&dt)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return
	}

	Logger.Log("[INFO]", fmt.Sprintf("%s", string(jsonData)))
}

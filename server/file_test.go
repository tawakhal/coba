package server

import (
	"encoding/json"
	"fmt"
	"testing"

	ul "git.bluebird.id/bluebird/util/log"
)

func TestWriteJSONFile(t *testing.T) {
	Logger = ul.StdLogger()

	var data []AddressLatLong
	for i := 1; i <= 10; i++ {
		data = append(data, AddressLatLong{
			Latitude:  float64(i * -1),
			Longitude: float64(i),
		})
	}
	err := writeJSONFile("test", data)
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log("[INFO]=", len(data))
	t.Fail()
}

func TestReadJSONFile(t *testing.T) {
	Logger = ul.StdLogger()

	opFile, err := readJSONFile("test.json")
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer opFile.Close()

	var aLatLong []AddressLatLong
	err = json.NewDecoder(opFile).Decode(&aLatLong)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Log("[INFO]=", fmt.Sprintf("%+v", aLatLong))

	t.Fail()
}

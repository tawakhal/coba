package server

import (
	"testing"

	ul "git.bluebird.id/bluebird/util/log"
)

const (
	devReverseGeo = "https://devmaps.bluebird.id/reverse/?format=json&lat=%g&lon=%g"
)

// TestingGetReverseGeoData asdasd
func TestGetReverseGeoData(t *testing.T) {
	Logger = ul.StdLogger()

	lat := -7.28911216
	lon := 112.81653728

	Logger.Log("[INFO]", "Server Started...")

	ms := NewMergingData(nil, nil, devReverseGeo)

	rsp, err := ms.GetReverseGeoData(lat, lon)
	if err != nil {
		t.Errorf("[ERROR]:%v\n", err)
		return
	}
	t.Logf("[INFO]:%+v\n", rsp)

	t.Fail()
}

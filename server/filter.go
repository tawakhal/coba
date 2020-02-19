package server

import (
	"fmt"

	pip "github.com/JamesMilnerUK/pip-go"
)

var areaFilter = 0

type filter struct {
	MapLat     map[string]struct{}
	MapLong    map[string]struct{}
	MapAddress map[string]struct{}
	Areas      []Area
}

func NewFilter(rw ReadWriter) (Filter, error) {
	MapLat := make(map[string]struct{})
	MapLong := make(map[string]struct{})
	MapAddress := make(map[string]struct{})

	areas, err := rw.ReadAreaIDNameLocsByAreaType(AreaTypeOperational)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return nil, err
	}
	Logger.Log("[INFO]", fmt.Sprintf("Area Location filter : %d", len(areas)))

	return &filter{MapLat: MapLat, MapLong: MapLong, MapAddress: MapAddress, Areas: areas}, nil
}

func (fl *filter) AddressExist(address string) bool {
	_, ok := fl.MapAddress[address]
	return ok
}

func (fl *filter) InsertAddressFilter(address string) {
	fl.MapAddress[address] = struct{}{}
}

func (fl *filter) InsertMultipleAddressFilter(address []string) {
	for _, v := range address {
		fl.MapAddress[v] = struct{}{}
	}
}

func (fl *filter) AddAreasForFilter(areas []Area) {
	for _, v := range fl.Areas {
		for _, k := range areas {
			if v.AreaName != k.AreaName {
				fl.InsertArea(k)
			} else {
				Logger.Log("[WARNING]", fmt.Sprintf("%s : %s [Its Same!!]", v.AreaName, k.AreaName))
			}
		}
	}
}

func (fl *filter) InsertArea(area Area) {
	fl.Areas = append(fl.Areas, area)
}

func (fl *filter) FilterAddresByArea(ads Address) Area {
	for _, v := range fl.Areas {
		plg := pip.Polygon{Points: v.Location}
		ok := pip.PointInPolygon(pip.Point{X: ads.Longitude, Y: ads.Latitude}, plg)
		if ok {
			return v
		}
	}
	areaFilter++
	Logger.Log("[WARNING]", fmt.Sprintf("Address : %+v, Not Have area !!", ads), "[COUNTER]", areaFilter)
	return Area{}
}

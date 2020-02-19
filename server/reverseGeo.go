package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ReverseGeoResp struct {
	PlaceID     int    `json:"place_id"`
	Licence     string `json:"licence"`
	OsmType     string `json:"osm_type"`
	OsmID       int    `json:"osm_id"`
	Lat         string `json:"lat"`
	Lon         string `json:"lon"`
	DisplayName string `json:"display_name"`
	Address     struct {
		Residential string `json:"residential"`
		Village     string `json:"village"`
		County      string `json:"county"`
		City        string `json:"city"`
		State       string `json:"state"`
		Country     string `json:"country"`
		CountryCode string `json:"country_code"`
	} `json:"address"`
	Boundingbox []string `json:"boundingbox"`
	OrderID int64
}

func (ms *mergingService) GetReverseGeoData(latitude, longitude float64) (ReverseGeoResp, error) {
	URL := fmt.Sprintf(ms.reverseGeoURL, latitude, longitude)

	httpreq, err := http.NewRequest("GET", URL, nil)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return ReverseGeoResp{}, err
	}

	client := &http.Client{}
	httpresp, err := client.Do(httpreq)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return ReverseGeoResp{}, err
	}
	defer httpresp.Body.Close()

	var resp ReverseGeoResp
	err = json.NewDecoder(httpresp.Body).Decode(&resp)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return ReverseGeoResp{}, err
	}
	return resp, nil
}

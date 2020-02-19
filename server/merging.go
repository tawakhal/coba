package server

import (
	"belajar/insertBigData/db"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"git.bluebird.id/bluebird/util/uuid"
)

type mergingService struct {
	writer        ReadWriter
	filter        Filter
	reverseGeoURL string
}

func NewMergingData(rw ReadWriter, fl Filter, reverseGeoURL string) MergingService {
	return &mergingService{writer: rw, filter: fl, reverseGeoURL: reverseGeoURL}
}

func (ms *mergingService) InsertMultipleData(ads []Address) error {
	now := time.Now()
	log.Printf("Len : %d, Time : %v", len(ads), now)

	if len(ads) > 0 {
		if len(ads)-1 >= db.MaxInsert {
			lop := math.Ceil(float64(len(ads)) / float64(db.MaxInsert))
			log.Println("len(ads) : ", len(ads))
			log.Println("db.MaxInsert : ", db.MaxInsert)
			log.Printf("len(ads) / db.MaxInsert : %f\n", float64(len(ads))/float64(db.MaxInsert))
			log.Println("Lop : ", lop)

			st, lt := 0, db.MaxInsert-1
			for i := 1; i <= int(lop); i++ {
				var dt []Address
				if i == int(lop) {
					dt = ads[st:]
					log.Printf("%d>[%d:%d]", i, st, len(ads))
				} else {
					dt = ads[st:lt]
					log.Printf("%d>[%d:%d]", i, st, lt)
				}

				err := ms.writer.BulkInsertAddresses(dt)
				if err != nil {
					Logger.Log("[ERROR]", err.Error())
					return err
				}
				st = lt
				lt += db.MaxInsert - 1
			}
		} else {
			err := ms.writer.BulkInsertAddresses(ads)
			if err != nil {
				Logger.Log("[ERROR]", err.Error())
				return err
			}
		}
	}

	log.Printf("Since : %f Seconds", time.Since(now).Seconds())
	return nil
}

func (ms *mergingService) InsertMultipleDataByCSV(path string, cust CustomInsert) error {
	// read CSV
	data, err := ReadCSV(path)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	log.Printf("Len of Csv : %d\n", len(data))

	// Read from db by city
	dt, err := ms.writer.ReadAddressesNameByCity(cust.City)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	// add to filter
	ms.filter.InsertMultipleAddressFilter(dt)
	log.Printf("Data from db : %d\n", len(dt))

	// Generate to slice of address
	ads := ms.generateAddressesByCSV(data, cust)

	err = ms.InsertMultipleData(ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) InsertMultipleDataByTSV(path string, skip int) error {
	data, err := ReadTSV(path, skip)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	log.Printf("Len of Tsv : %d\n", len(data))

	if len(data) == 1 || len(data) < 2 {
		Logger.Log("[WARNING]", "Skip is wrong value")
		return nil
	}

	// Read from db by city
	dt, err := ms.writer.ReadAdresssesName()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	// add to filter
	ms.filter.InsertMultipleAddressFilter(dt)
	log.Printf("Data from db : %d\n", len(dt))

	ads := ms.generateAddressesByTSV(data)

	err = ms.InsertMultipleData(ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil

}

func (ms *mergingService) InsertMultipleDataByJson(path string) error {
	opFile, err := readJSONFile(path)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	defer opFile.Close()

	var revGeoResp []ReverseGeoResp
	err = json.NewDecoder(opFile).Decode(&revGeoResp)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", "Data from Json", "[LENGTH]", fmt.Sprintf("%d", len(revGeoResp)))

	// Get all data from db
	dt, err := ms.writer.ReadAdresssesName()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", "Data from db", "[LENGTH]", fmt.Sprintf("%d", len(dt)))

	// add to filter
	ms.filter.InsertMultipleAddressFilter(dt)

	ads, err := ms.generateAddressesByReverseGeo(revGeoResp)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", fmt.Sprintf("Data will insert : %d", len(ads)))

	err = ms.InsertMultipleData(ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) GenerateOSMTSVFile(nameFile string) error {
	if nameFile == "" {
		nameFile = fmt.Sprintf("%v", time.Now())
	}

	// Get all data from db
	ads, err := ms.writer.ReadOSMAddresses()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", "Data from db", "[LENGTH]", fmt.Sprintf("%d", len(ads)))

	// move to another struct
	var osmData []OsmTsv
	for _, v := range ads {
		osmData = append(osmData, OsmTsv{
			name:              v.Name,
			alternative_names: v.AlternativeName,
			osm_type:          v.OsmType,
			osm_id:            v.AddressID,
			class:             v.Class,
			Type:              v.Type,
			lon:               fmt.Sprintf("%f", v.Longitude),
			lat:               fmt.Sprintf("%f", v.Latitude),
			place_rank:        fmt.Sprint(v.PlaceRank),
			importance:        fmt.Sprintf("%f", v.Importance),
			street:            v.Street,
			city:              v.City,
			county:            v.County,
			state:             v.State,
			country:           v.Country,
			country_code:      v.CountryCode,
			display_name:      v.DisplayName,
			west:              fmt.Sprintf("%f", v.West),
			south:             fmt.Sprintf("%f", v.South),
			east:              fmt.Sprintf("%f", v.East),
			north:             fmt.Sprintf("%f", v.North),
			wikidata:          v.Wikidata,
			// wikipedia :  v.   ,
			housenumbers: fmt.Sprint(v.HouseNumber),
		})
	}

	err = createTSVFile(nameFile, osmData)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) MigrateAddressTrxLocationToMstAddressByAreaName(areaName string) error {
	//
	areaID, err := ms.writer.ReadAreaIDByAreaName(areaName)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	if areaID.MSB == 0 || areaID.LSB == 0 {
		Logger.Log("[CRITICAL]", "AreaID is Null", "[AREANAME]", areaName)
		return nil
	}

	adll, err := ms.writer.ReadAddressLatLongFilterAddressLatLongNullByAreaID(areaID)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", fmt.Sprintf("Data %s", areaName), "[LENGTH]", fmt.Sprintf("%d", len(adll)))

	areaName = strings.ToLower(areaName)

	// Read from db by city
	dt, err := ms.writer.ReadAddressesNameByCity(areaName)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	// add to filter
	ms.filter.InsertMultipleAddressFilter(dt)
	Logger.Log("[INFO]", fmt.Sprintf("Data from db : %d", len(dt)))

	ads := ms.generateAddressesByADLL(adll, areaName, areaID)

	err = ms.InsertMultipleData(ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) CreateFileTrxLocationHailing() error {
	// read all data hailing
	aLatLons, err := ms.writer.ReadLatLongTrxLocHailing()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", fmt.Sprint("Data Hailing..."), "[LENGTH]", fmt.Sprintf("%d", len(aLatLons)))

	err = writeJSONFile(fmt.Sprintf("%d-trx_locs", time.Now().Unix()), aLatLons)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) CreateReverseGeoJSONByTrxLocationJSON(nameJSON string) error {
	opFile, err := readJSONFile(nameJSON)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	defer opFile.Close()

	var trxLocs []AddressLatLong
	err = json.NewDecoder(opFile).Decode(&trxLocs)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	k := 1
	now := time.Now()
	var rgresp []ReverseGeoResp
	for i, v := range trxLocs {
		rsp, err := ms.GetReverseGeoData(v.Latitude, v.Longitude)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return err
		}
		rsp.OrderID = v.OrderID

		rgresp = append(rgresp, rsp)
		if i+1 == k*50 || i+1 == len(trxLocs) {
			Logger.Log("[INFO]", fmt.Sprintf("Reverse-Geo:%d", i+1), "[TIME]", fmt.Sprintf("%f seconds", time.Since(now).Seconds()))
			now = time.Now()
			k++
		}
	}

	ads, err := ms.generateAddressesByReverseGeo(rgresp)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		// return err
	}

	err = writeJSONFile(fmt.Sprintf("%d-reverse_geo", time.Now().Unix()), ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) MigrateAddressTrxLocationHailing() error {
	dt, err := ms.writer.ReadAdresssesName()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	// add to filter
	ms.filter.InsertMultipleAddressFilter(dt)
	Logger.Log("[INFO]", fmt.Sprintf("Data from db : %d", len(dt)))

	// read all data hailing
	aLatLons, err := ms.writer.ReadLatLongTrxLocHailing()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", fmt.Sprint("Data Hailing..."), "[LENGTH]", fmt.Sprintf("%d", len(aLatLons)))

	k := 1
	now := time.Now()
	var rgresp []ReverseGeoResp
	for i, v := range aLatLons {
		rsp, err := ms.GetReverseGeoData(v.Latitude, v.Longitude)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return err
		}

		rgresp = append(rgresp, rsp)
		if i+1 == k*50 || i+1 == len(aLatLons) {
			Logger.Log("[INFO]", fmt.Sprintf("Reverse-Geo:%d", i+1), "[TIME]", fmt.Sprintf("%f seconds", time.Since(now).Seconds()))
			now = time.Now()
			k++
		}
	}

	ads, err := ms.generateAddressesByReverseGeo(rgresp)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	err = ms.InsertMultipleData(ads)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return nil
}

func (ms *mergingService) FixingAreaIDCityALLAddress() error {

	ads, err := ms.writer.ReadIDLatLong()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	Logger.Log("[INFO]", fmt.Sprintf("Data from db : %d", len(ads)))

	for _, v := range ads {
		area := ms.filter.FilterAddresByArea(v)

		err = ms.writer.UpdateAreaIDCItyMstAddressByID(Address{AreaID: area.AreaID, City: area.AreaName})
		if err != nil {
			Logger.Log("[ERROR]", err.Error(), "[DATA]", fmt.Sprintf("%+v", v))
		}
	}
	return nil
}

func (ms *mergingService) generateAddressesByCSV(data [][]string, cust CustomInsert) []Address {
	var ads []Address
	var kak int
	for _, v := range data {
		adres := fmt.Sprintf("%v", v[2])
		adres = cleanDrityWord(adres)
		// adres = completeAddress(adres, cust.City, cust.Country)
		if !ms.filter.AddressExist(adres) {
			ms.filter.InsertAddressFilter(adres)

			adres = strings.ReplaceAll(adres, "'", " ")

			lat, _ := strconv.ParseFloat(v[3], 64)
			lon, _ := strconv.ParseFloat(v[4], 64)

			dt := Address{
				Name:            adres,
				AlternativeName: adres,
				AreaID:          cust.AreaID,
				OsmType:         cust.OsmType,
				Class:           cust.Class,
				Type:            cust.Type,
				Latitude:        lat,
				Longitude:       lon,
				PlaceRank:       DefaultPlaceRank,
				Importance:      DefaultImpotance,
				// Street : "",
				City: cust.City,
				// County : "",
				// State : "",
				Country:     DefaultCountry,
				CountryCode: DefaultCountryCode,
				DisplayName: adres,
				// West : ,
				// South : ,
				// East : ,
				// North : ,
				// Wikidata : ,
				// HouseNumber				 : ,
			}

			if cust.CsvType == CsvType2 {
				if v[6] != "" {
					dt.City = v[6]
				}
			}

			if cust.Country != "" {
				dt.Country = cust.Country
			}

			if cust.CountryCode != "" {
				dt.CountryCode = cust.CountryCode
			}

			ads = append(ads, dt)
		} else {
			kak++
		}
	}

	log.Printf("Geting filter : %d\n", kak)
	return ads
}

func (ms *mergingService) generateAddressesByTSV(data []OsmTsv) []Address {
	var ads []Address
	var kak int
	for _, v := range data {
		adres := fmt.Sprintf("%v", v.display_name)
		adres = cleanDrityWord(adres)
		// adres = completeAddress(adres, cust.City, cust.Country)
		if !ms.filter.AddressExist(adres) {
			ms.filter.InsertAddressFilter(adres)

			adres = strings.ReplaceAll(adres, "'", " ")
			st := strings.ReplaceAll(v.state, "'", " ")
			srett := strings.ReplaceAll(v.street, "'", " ")
			cty := strings.ReplaceAll(v.city, "'", " ")
			conty := strings.ReplaceAll(v.county, "'", " ")
			wikidata := strings.ReplaceAll(v.wikidata, "'", " ")
			osmtype := strings.ReplaceAll(v.osm_type, "'", " ")
			class := strings.ReplaceAll(v.class, "'", " ")

			lat, _ := strconv.ParseFloat(v.lat, 64)
			lon, _ := strconv.ParseFloat(v.lon, 64)

			west, _ := strconv.ParseFloat(v.west, 64)
			south, _ := strconv.ParseFloat(v.south, 64)
			east, _ := strconv.ParseFloat(v.east, 64)
			north, _ := strconv.ParseFloat(v.north, 64)

			housenumbers, _ := strconv.ParseInt(v.housenumbers, 10, 64)

			dt := Address{
				Name:            adres,
				AlternativeName: adres,
				// AreaID:          cust.AreaID,
				OsmType:     osmtype,
				Class:       class,
				Type:        AddressTypeWay.String(),
				Latitude:    lat,
				Longitude:   lon,
				PlaceRank:   DefaultPlaceRank,
				Importance:  DefaultImpotance,
				Street:      srett,
				City:        cty,
				County:      conty,
				State:       st,
				Country:     DefaultCountry,
				CountryCode: DefaultCountryCode,
				DisplayName: adres,
				West:        west,
				South:       south,
				East:        east,
				North:       north,
				Wikidata:    wikidata,
				HouseNumber: int32(housenumbers),
			}

			ads = append(ads, dt)
		} else {
			kak++
		}
	}
	log.Printf("Geting filter : %d\n", kak)
	return ads
}

func (ms *mergingService) generateAddressesByADLL(data []AddressLatLong, areaName string, AreaID uuid.UUID) []Address {
	var ads []Address
	var kak int
	for _, v := range data {
		adres := strings.ReplaceAll(v.Address, "'", " ")
		adres = cleanDrityWord(adres)
		// adres = completeAddress(adres, cust.City, cust.Country)
		if !ms.filter.AddressExist(adres) {
			ms.filter.InsertAddressFilter(adres)

			dt := Address{
				Name:            adres,
				AlternativeName: adres,
				AreaID:          AreaID,
				OsmType:         DefaultOSMType,
				Class:           DefaultOSMClass,
				Type:            DefaultAddressType,
				Latitude:        v.Latitude,
				Longitude:       v.Longitude,
				PlaceRank:       DefaultPlaceRank,
				Importance:      DefaultImpotance,
				// Street : "",
				City: areaName,
				// County : "",
				// State : "",
				Country:     DefaultCountry,
				CountryCode: DefaultCountryCode,
				DisplayName: adres,
				// West : ,
				// South : ,
				// East : ,
				// North : ,
				// Wikidata : ,
				// HouseNumber				 : ,
			}

			ads = append(ads, dt)
		} else {
			kak++
		}
	}
	log.Printf("Geting filter : %d\n", kak)
	return ads
}

func (ms *mergingService) generateAddressesByReverseGeo(data []ReverseGeoResp) ([]Address, error) {
	var ads []Address
	var kak int
	for _, v := range data {
		adres := strings.ReplaceAll(v.DisplayName, "'", " ")
		adres = cleanDrityWord(adres)
		if !ms.filter.AddressExist(adres) {
			ms.filter.InsertAddressFilter(adres)

			lat, err := strconv.ParseFloat(v.Lat, 64)
			if err != nil {
				Logger.Log("[ERROR]", err.Error(), "[DATA]", fmt.Sprintf("%+v", v))
				continue
			}
			long, err := strconv.ParseFloat(v.Lon, 64)
			if err != nil {
				Logger.Log("[ERROR]", err.Error(), "[DATA]", fmt.Sprintf("%+v", v))
				continue
			}

			dt := Address{
				Name:            adres,
				AlternativeName: adres,
				// AreaID:          AreaID,
				OsmType:    v.OsmType,
				Class:      DefaultOSMClass,
				Type:       DefaultAddressType,
				Latitude:   lat,
				Longitude:  long,
				PlaceRank:  DefaultPlaceRank,
				Importance: DefaultImpotance,
				// Street : "",
				City:        v.Address.City,
				County:      v.Address.County,
				State:       v.Address.State,
				Country:     v.Address.Country,
				CountryCode: v.Address.CountryCode,
				DisplayName: adres,
				// Wikidata : ,
				// HouseNumber				 : ,
			}

			for i, _ := range v.Boundingbox {
				bound, err := strconv.ParseFloat(v.Boundingbox[i], 64)
				if err != nil {
					Logger.Log("[ERROR]", err.Error(), "[DATA]", fmt.Sprintf("%+v", v))
					continue
				}
				switch i {
				case 0:
					dt.West = bound
				case 1:
					dt.South = bound
				case 2:
					dt.East = bound
				case 3:
					dt.North = bound
				}
			}

			ads = append(ads, dt)
			Logger.Log("[INFO]", fmt.Sprintf("asss : %d", len(ads)))
		} else {
			kak++
		}
	}
	Logger.Log("[INFO]", fmt.Sprintf("Geting filter : %d", kak))
	return ads, nil
}

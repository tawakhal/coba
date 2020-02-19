package server

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"strings"

	"github.com/JamesMilnerUK/pip-go"

	"git.bluebird.id/bluebird/util/uuid"

	_ "github.com/go-sql-driver/mysql" //mysql driver
)

const (
	// === area.mst_address
	// INSERT
	insertAddress         = `INSERT INTO mst_address(name, alternative_name, area_msb, area_lsb, osm_type, class, type, latitude, longitude, place_rank, importance, street, city, county, state, country, country_code, display_name, west, south, east, north, wikidata, house_number) VALUES `
	valuesOfInsertAddress = `('%s', '%s', %d, %d, '%s', '%s', '%s', %f, %f, %d, %f, '%s', '%s', '%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, '%s', %d)`
	// SELECT
	selectAddressNameByCity = `SELECT name FROM mst_address WHERE city=?`
	selectAdressName        = `SELECT name FROM mst_address `
	selectOSMAdresses       = `SELECT address_id,name,alternative_name,osm_type,class,type,latitude,longitude,place_rank,
	importance,street,city,county,state,country,country_code,display_name,west,south,east,north,wikidata,house_number FROM mst_address`
	selectIDLatLong = `SELECT address_id, latitude, longitude FROM mst_address `
	// UPDATE
	updateAreaIDCityMstAddressByID = `UPDATE mst_address SET area_msb =?, area_lsb =?, city =? WHERE address_id=?`

	// === area.mst_area
	selectAreaIDByAreaName         = `SELECT id_msb, id_lsb FROM mst_area WHERE area_name = ?`
	selectAreaIDNameLocsByAreaType = `SELECT id_msb, id_lsb, area_name, ST_AsWKB(location) FROM mst_area WHERE area_type=?`
	selectLocationByAreaName       = `SELECT ST_AsWKB(location) FROM mst_area WHERE area_name= ?`

	// === orders.trx_location
	selectAddressLatLongFilterAddressLatLongNullByAreaID = `SELECT address,latitude,longitude FROM  orders.trx_location  WHERE  area_msb=? AND area_lsb=? AND (LENGTH(address) > 1 AND latitude != 0 AND longitude != 0);`

	selectLatLongTrxLocationByAddressNull = `SELECT order_id, latitude, longitude FROM orders.trx_location WHERE address = '' ORDER BY order_id ASC`
)

type dbReadWriter struct {
	db *sql.DB
}

func NewDBReadWriter(url string, schema string, user string, password string) ReadWriter {
	schemaURL := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, password, url, schema)
	db, err := sql.Open("mysql", schemaURL)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
	}
	return &dbReadWriter{db: db}
}

func (rw *dbReadWriter) BulkInsertAddresses(ads []Address) error {
	tx, err := rw.db.Begin()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	defer tx.Rollback()

	var instSQL []string
	for _, v := range ads {
		sl := fmt.Sprintf(valuesOfInsertAddress, v.Name, v.AlternativeName, v.AreaID.MSB, v.AreaID.LSB, v.OsmType, v.Class, v.Type,
			v.Latitude, v.Longitude, v.PlaceRank, v.Importance, v.Street, v.City, v.County, v.State, v.Country, v.CountryCode, v.DisplayName,
			v.West, v.South, v.East, v.North, v.Wikidata, v.HouseNumber)
		instSQL = append(instSQL, sl)
	}

	SQL := insertAddress + strings.Join(instSQL, ",")
	_, err = tx.Exec(SQL)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		// Logger.Log("[SQL]", SQL)
		return err
	}

	return tx.Commit()
}

func (rw *dbReadWriter) ReadAddressesNameByCity(city string) ([]string, error) {
	var names []string

	rows, err := rw.db.Query(selectAddressNameByCity, city)
	if err == sql.ErrNoRows {
		log.Println("Data Not Exist - ", city)
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return names, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func (rw *dbReadWriter) ReadAdresssesName() ([]string, error) {
	var names []string

	rows, err := rw.db.Query(selectAdressName)
	if err == sql.ErrNoRows {
		log.Println("Data Not Exist - ")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return names, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

func (rw *dbReadWriter) ReadOSMAddresses() ([]Address, error) {
	var ads []Address

	rows, err := rw.db.Query(selectOSMAdresses)
	if err == sql.ErrNoRows {
		log.Println("Data Not Exist - ")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return ads, err
	}
	defer rows.Close()

	for rows.Next() {
		var adr Address
		err = rows.Scan(&adr.AddressID, &adr.Name, &adr.AlternativeName, &adr.OsmType, &adr.Class,
			&adr.Type, &adr.Latitude, &adr.Longitude, &adr.PlaceRank, &adr.Importance, &adr.Street,
			&adr.City, &adr.County, &adr.State, &adr.CountryCode, &adr.CountryCode, &adr.DisplayName,
			&adr.West, &adr.South, &adr.East, &adr.North, &adr.Wikipedia, &adr.HouseNumber)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return ads, err
		}
		ads = append(ads, adr)
	}
	return ads, nil
}

func (rw *dbReadWriter) ReadAreaIDByAreaName(areaName string) (uuid.UUID, error) {
	var areaID uuid.UUID

	err := rw.db.QueryRow(selectAreaIDByAreaName, areaName).Scan(&areaID.MSB, &areaID.LSB)
	if err == sql.ErrNoRows {
		Logger.Log("[WARNING]", fmt.Sprint("Data Not Exist - ", areaName))
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return areaID, err
	}
	return areaID, nil
}

func (rw *dbReadWriter) ReadAddressLatLongFilterAddressLatLongNullByAreaID(areaID uuid.UUID) ([]AddressLatLong, error) {
	var adll []AddressLatLong

	rows, err := rw.db.Query(selectAddressLatLongFilterAddressLatLongNullByAreaID, areaID.MSB, areaID.LSB)
	if err == sql.ErrNoRows {
		Logger.Log("[WARNING]", fmt.Sprintf("Data Not Exist - [%d:%d]", areaID.MSB, areaID.LSB))
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return adll, err
	}
	defer rows.Close()

	for rows.Next() {
		var ad AddressLatLong
		err = rows.Scan(&ad.Address, &ad.Latitude, &ad.Longitude)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return adll, err
		}
		adll = append(adll, ad)
	}
	return adll, nil
}

func (rw *dbReadWriter) ReadLatLongTrxLocHailing() ([]AddressLatLong, error) {
	var adll []AddressLatLong

	rows, err := rw.db.Query(selectLatLongTrxLocationByAddressNull)
	if err == sql.ErrNoRows {
		Logger.Log("[WARNING]", "Data Not Exist")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return adll, err
	}
	defer rows.Close()

	for rows.Next() {
		var adl AddressLatLong
		err = rows.Scan(&adl.OrderID, &adl.Latitude, &adl.Longitude)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return adll, err
		}
		adll = append(adll, adl)
	}
	return adll, nil
}

func (rw *dbReadWriter) UpdateAreaIDCItyMstAddressByID(req Address) error {
	tx, err := rw.db.Begin()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(updateAreaIDCityMstAddressByID, req.AreaID.MSB, req.AreaID.LSB, req.City, req.AddressID)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return err
	}

	return tx.Commit()
}

func (rw *dbReadWriter) ReadAreaIDNameLocsByAreaType(areaType int32) ([]Area, error) {
	var area []Area

	rows, err := rw.db.Query(selectAreaIDNameLocsByAreaType, areaType)
	if err == sql.ErrNoRows {
		Logger.Log("[WARNING]", "Data Not Exist")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return area, err
	}
	defer rows.Close()

	for rows.Next() {
		var ar Area
		var pts []byte
		err = rows.Scan(&ar.AreaID.MSB, &ar.AreaID.LSB, &ar.AreaName, &pts)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return area, err
		}
		po := []pip.Point{}
		if pts != nil {
			po = decodeGeoData(pts)
		}
		ar.Location = po

		area = append(area, ar)
	}
	return area, nil
}

func (rw *dbReadWriter) ReadIDLatLong() ([]Address, error) {
	var ads []Address

	rows, err := rw.db.Query(selectIDLatLong)
	if err == sql.ErrNoRows {
		log.Println("Data Not Exist - ")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return ads, err
	}
	defer rows.Close()

	for rows.Next() {
		var ad Address
		err = rows.Scan(&ad.AddressID, &ad.Latitude, &ad.Longitude)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			return ads, err
		}
		ads = append(ads, ad)
	}
	return ads, err
}

func (rw *dbReadWriter) ReadLocationByAreaName(areaName string) ([]pip.Point, error) {
	var pts []byte
	err := rw.db.QueryRow(selectLocationByAreaName, areaName).Scan(&pts)
	if err == sql.ErrNoRows {
		log.Println("Data Not Exist - ")
	} else if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return nil, err
	}

	var locs []pip.Point
	if pts != nil {
		locs = decodeGeoData(pts)
	}
	return locs, nil
}

// decodeGeoData decodes input to Point.
func decodeGeoData(gd []byte) []pip.Point {
	var pts []pip.Point

	brd := bytes.NewReader(gd)

	//determine the byte endian ness:
	if gd[0] == 1 {
		//skip the first byte (already read in gd[0])
		brd.Seek(1, 0)

		//read the next 4 bytes, for object type:
		var d1 uint32
		binary.Read(brd, binary.LittleEndian, &d1)

		switch d1 {
		case 1:
			// log.Println("type is POINT")
		case 2:
			// log.Println("type is LINESTRING")
		case 3:
			// log.Println("type is POLYGON")
		case 4:
			// log.Println("type is MULTIPOINT")

		}

		//read the next 4 bytes, number of polygons
		var d2 uint32
		binary.Read(brd, binary.LittleEndian, &d2)

		// log.Println("Number of POLYGONS =", d2)

		//read the next 4 bytes, number of points in polygon
		var d3 uint32
		binary.Read(brd, binary.LittleEndian, &d3)

		// log.Println("Number of POINTS =", d3)

		//loop for the number of POINTS:
		for i := 0; i < int(d3); i++ {
			var pt pip.Point
			var d4 float64

			//for each point, read twice for lat/lon (x/y):
			binary.Read(brd, binary.LittleEndian, &d4)
			pt.Y = d4

			binary.Read(brd, binary.LittleEndian, &d4)
			pt.X = d4

			// fmt.Printf("...POINTS %d : %v\n", i+1, pt)
			pts = append(pts, pt)
		}

	}
	return pts

}

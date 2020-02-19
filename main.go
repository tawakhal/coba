package main

import (
	"flag"
	"fmt"
	"time"

	sv "git.bluebird.id/bluebird/area/client/merging-data/v2/server"
	cfg "git.bluebird.id/bluebird/util/config"
	ul "git.bluebird.id/bluebird/util/log"
)

const (
	url      = "172.26.11.230:3306"
	schema   = "area"
	user     = "dev"
	password = "d3v"

	// Reverse geo devmaps
	devReverseGeo = "https://devmaps.bluebird.id/reverse/?format=json&lat=%g&lon=%g"
	stgReverseGeo = "https://stgmaps.bluebird.id/reverse/?format=json&lat=%g&lon=%g"
)

// This for insert
func main() {
	sv.Logger = ul.StdLogger()
	confFlag := flag.String("config", "", "config file")
	flag.Parse()

	var ok bool
	if len(*confFlag) == 0 {
		ok = cfg.AppConfig.LoadConfig()
	} else {
		ok = cfg.AppConfig.LoadConfigFile(*confFlag)
	}
	if !ok {
		fmt.Println("[ERROR]", "failed to load configuration")
		return
	}

	fmt.Println("[WELCOME] Client for Orcomp")
	fmt.Println("[!!] Semua config berada di service.conf")
	fmt.Println("[!!] Bisa juga menggunakan -config \"path dari file config\"")
	fmt.Println()
	fmt.Println("[SERVICE.CONF] INFORMASI")
	fmt.Println("[*] \"automatic\" adalah otomatis tanpa menggunakan service.conf untuk pilihan nomor")
	fmt.Println("[***] 0 = Otomatis")
	fmt.Println("[***] 1 = Input manual")
	fmt.Println("[*] \"reversegeotype\" server reverse geo yang ingin digunakan")
	fmt.Println("[***] 1 = develop")
	fmt.Println("[***] 2 = staging")
	fmt.Println("[*] \"typefile\" itu adalah type file yang ingin digunakan")
	fmt.Println("[***] 1 = database")
	fmt.Println("[***] 2 = csv")
	fmt.Println("[***] 3 = tsv")
	fmt.Println("[***] 4 = json(reverse-geo)")
	fmt.Println("[*] \"path\" itu adalah tempat file disimpan. ex: home/olgi/nama_file.json")

	// DB
	dbhost := cfg.Get("dbhost", "")
	dbname := cfg.Get("dbname", "")
	dbuser := cfg.Get("dbuser", "")
	dbpwd := cfg.Get("dbpwd", "")

	// Reverse Geo Server
	reversegeotype := cfg.GetI("reversegeotype", 1)
	devreversegeo := cfg.Get("devreversegeo", "")
	stgreversegeo := cfg.Get("stgreversegeo", "")

	var reverseGeoURL string
	switch reversegeotype {
	case 1:
		reverseGeoURL = devreversegeo
	case 2:
		reverseGeoURL = stgreversegeo
	default:
		fmt.Println("reversegeotype not found :['", reversegeotype, "']")
		return
	}
	fmt.Println("[ReverseGeoURL] : ", reverseGeoURL)

	// Type file
	typefile := cfg.GetI("typefile", 0)
	pathFile := cfg.Get("pathfile", "")

	if typefile == 0 {
		fmt.Println("Typefile not found :['", typefile, "']")
		return
	}
	fmt.Println("[typefile] : ", typefile)

	if pathFile == "" {
		fmt.Println("pathFile not found :[", pathFile, "]")
		return
	}
	fmt.Println("[pathFile] : ", pathFile)

	// initial db
	rw := sv.NewDBReadWriter(dbhost, dbname, dbuser, dbpwd)
	fmt.Println("[DB] is running...")

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}
	fmt.Println("[FILTER] is running...")

	ms := sv.NewMergingData(rw, fl, devReverseGeo)
	fmt.Println("[MERGING] is running...")

	automatic := cfg.GetI("automatic", 0)

	fmt.Println()
	var nopilihan int
	if automatic == 1 {
		fmt.Println("[SERVICE] OTOMATIS")
		nopilihan = cfg.GetI("nopilihan", 0)
	} else {
		fmt.Println("[SERVICE] MANUAL")
	}

	// var cust sv.CustomInsert
	// custominsert := cfg.GetI("custominsert", 0)
	// if custominsert == 1 {
	// 	fmt.Println("[SERVICE] OTOMATIS")
	// 	nopilihan = cfg.GetI("nopilihan", 0)
	// 	idMSB := cfg.GetI("idMSB", 0)
	// 	idLSB := cfg.GetI("idLSB", 0)
	// 	OsmType := cfg.GetI("OsmType", 0)
	// 	Class := cfg.GetI("Class", 0)
	// 	Type := cfg.GetI("Type", 0)
	// 	City := cfg.GetI("City", 0)
	// 	Country := cfg.GetI("Country", 0)
	// 	CountryCode := cfg.GetI("CountryCode", 0)
	// 	CsvType := cfg.GetI("CsvType", 0)
	// 	cust.AreaID = uuid.UUID{MSB: idMSB, LSB: idLSB}
	// 	cust.OsmType = sv.OsmType(OsmType)
	// 	cust.Class = Class
	// 	cust.Type = Type
	// 	cust.City = City
	// 	cust.Country = Country
	// 	cust.CountryCode = CountryCode
	// 	cust.CsvType = CsvType
	// }

	fmt.Println("[INFO] Mengunakanan Pilihan : ", nopilihan)

	fmt.Println("[SERVICE] Starting...")

	// InsertDataByCSVPath(pathFile, rw, ms)

	// InsertdataByTSVPath()

	GenerateFileTSV(rw, ms)

	// jakarta,cilegon,semarang, bali, surabaya, medan
	// MigrateData("jakarta")

	// MigrateDataHailing()

	// CreateFileTrxLocsHailing()

	// InsertdataByJSONPath(pathFile, rw, ms)

	// CreateJSONtrxLocs(reverseGeoURL)

	// FixingAreaIDCity(rw, ms)

}

func InsertDataByCSVPath(path string, rw sv.ReadWriter, ms sv.MergingService) {

	cust := sv.CustomInsert{
		// AreaID:  uuid.UUID{MSB: 17616523481768870110, LSB: 13668126136777751731},
		OsmType:     sv.OsmTypeWay.String(),
		Class:       sv.AddressClassAdministrative.String(),
		Type:        sv.AddressTypeWay.String(),
		City:        "bandung",
		Country:     "indonesia",
		CountryCode: "id",
	}
	err := ms.InsertMultipleDataByCSV(path, cust)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}
}

func InsertdataByTSVPath() {
	// Read data csv from Jakarta.csv (2019-09-16)
	path := "/home/olgi/Project/Go/src/git.bluebird.id/bluebird/area/client/merging-data/data/tsv/20190201/indonesia.tsv"

	// initial db
	rw := sv.NewDBReadWriter(url, schema, user, password)

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	ms := sv.NewMergingData(rw, fl, devReverseGeo)

	err = ms.InsertMultipleDataByTSV(path, 0)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}
}

func InsertdataByJSONPath(path string, rw sv.ReadWriter, ms sv.MergingService) {

	err := ms.InsertMultipleDataByJson(path)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}
}

func GenerateFileTSV(rw sv.ReadWriter, ms sv.MergingService) {
	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	if err := ms.GenerateOSMTSVFile(fmt.Sprintf("%v", time.Now())); err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}
	sv.Logger.Log("[TIME]", fmt.Sprintf("%f seconds", time.Since(now).Seconds()))
}

func MigrateData(areaName string) {

	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	// initial db
	rw := sv.NewDBReadWriter(url, schema, user, password)

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	ms := sv.NewMergingData(rw, fl, devReverseGeo)

	err = ms.MigrateAddressTrxLocationToMstAddressByAreaName(areaName)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	sv.Logger.Log("[TIME]", fmt.Sprintf("%v seconds", time.Since(now).Seconds()))
}

func MigrateDataHailing() {
	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	// initial db
	rw := sv.NewDBReadWriter(url, schema, user, password)

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	ms := sv.NewMergingData(rw, fl, devReverseGeo)

	err = ms.MigrateAddressTrxLocationHailing()
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	sv.Logger.Log("[TIME]", fmt.Sprintf("%v seconds", time.Since(now).Seconds()))
}

func CreateFileTrxLocsHailing() {
	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	// initial db
	rw := sv.NewDBReadWriter(url, schema, user, password)

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	ms := sv.NewMergingData(rw, fl, devReverseGeo)

	err = ms.CreateFileTrxLocationHailing()
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	sv.Logger.Log("[TIME]", fmt.Sprintf("%v seconds", time.Since(now).Seconds()))
}

func CreateJSONtrxLocs(reverseGeoURL string) {
	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	// initial db
	rw := sv.NewDBReadWriter(url, schema, user, password)

	// initial filter
	fl, err := sv.NewFilter(rw)
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	ms := sv.NewMergingData(rw, fl, reverseGeoURL)

	err = ms.CreateReverseGeoJSONByTrxLocationJSON("trx_locs.json")
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	sv.Logger.Log("[TIME]", fmt.Sprintf("%v seconds", time.Since(now).Seconds()))
}

func FixingAreaIDCity(rw sv.ReadWriter, ms sv.MergingService) {
	now := time.Now()
	sv.Logger.Log("[INFO]", "Server Started...")

	err := ms.FixingAreaIDCityALLAddress()
	if err != nil {
		sv.Logger.Log("[ERROR]", err.Error())
		return
	}

	sv.Logger.Log("[TIME]", fmt.Sprintf("%v seconds", time.Since(now).Seconds()))
}

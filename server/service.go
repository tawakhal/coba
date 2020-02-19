package server

import (
	"git.bluebird.id/bluebird/util/uuid"
	"github.com/JamesMilnerUK/pip-go"
	logkit "github.com/go-kit/kit/log"
)

const (
	MaxInsert = 1250

	DefaultPlaceRank = 2

	DefaultImpotance = 0.7

	DefaultCountry     = "indonesia"
	DefaultCountryCode = "id"
	DefaultOSMType     = "way"
	DefaultOSMClass    = "administrative"
	DefaultAddressType = "way"

	AreaTypeOperational = 0
)

var (
	dirtyWord = []string{">", "<", "=", "#", "+", "\\"}
)

type OsmType string

// For OsmType
const (
	OsmTypeWay      OsmType = "way"
	OsmTypeRelation OsmType = "relation"
	OsmTypeNode     OsmType = "node"
)

func (ot OsmType) String() string {
	return string(ot)
}

type AddressClass string

// For AddressClass
const (
	AddressClassAdministrative AddressClass = "administrative"
	AddressClassCity           AddressClass = "city"
	AddressClassSuburb         AddressClass = "suburb"
	AddressClassState          AddressClass = "state"
	AddressClassOperational    AddressClass = "operational"
	AddressClassPeak           AddressClass = "peak"
	AddressClassVolcano        AddressClass = "volcano"
	AddressClassVillage        AddressClass = "village"
	AddressClassResidential    AddressClass = "residential"
)

func (ac AddressClass) String() string {
	return string(ac)
}

type AddressType string

// For AddressType
const (
	AddressTypeWay AddressType = "way"
)

func (at AddressType) String() string {
	return string(at)
}

type CSVType int32

// For CSVType structure
const (
	CsvType1 CSVType = 1 // customer_name, phone, address, cust_latitude, cust_longitude, province
	CsvType2 CSVType = 2 // customer_name, phone, got_on_addess, cust_latitude, cust_longitude, province, contact_center, status_name
)

// === all variable
var (
	Logger logkit.Logger
)

// === all struct

type Address struct {
	AddressID       string
	Name            string
	AlternativeName string
	AreaID          uuid.UUID
	OsmType         string
	Class           string
	Type            string
	Latitude        float64
	Longitude       float64
	PlaceRank       int32
	Importance      float64
	Street          string
	City            string
	County          string
	State           string
	Country         string
	CountryCode     string
	DisplayName     string
	West            float64
	South           float64
	East            float64
	North           float64
	Wikidata        string
	Wikipedia       string
	HouseNumber     int32
}

type OsmTsv struct {
	name              string `tsv:"name"`
	alternative_names string `tsv:"alternative_names"`
	osm_type          string `tsv:"osm_type"`
	osm_id            string `tsv:"osm_id"`
	class             string `tsv:"class"`
	Type              string `tsv:"type"`
	lon               string `tsv:"lon"`
	lat               string `tsv:"lat"`
	place_rank        string `tsv:"place_rank"`
	importance        string `tsv:"importance"`
	street            string `tsv:"street"`
	city              string `tsv:"city"`
	county            string `tsv:"county"`
	state             string `tsv:"state"`
	country           string `tsv:"country"`
	country_code      string `tsv:"country_code"`
	display_name      string `tsv:"display_name"`
	west              string `tsv:"west"`
	south             string `tsv:"south"`
	east              string `tsv:"east"`
	north             string `tsv:"north"`
	wikidata          string `tsv:"wikidata"`
	wikipedia         string `tsv:"wikipedia"`
	housenumbers      string `tsv:"housenumbers"`
}

type AddressLatLong struct {
	Address   string
	OrderID   int64
	Latitude  float64
	Longitude float64
}

type CustomInsert struct {
	AreaID      uuid.UUID
	OsmType     string
	Class       string
	Type        string
	City        string
	Country     string
	CountryCode string
	CsvType     CSVType
}

type Area struct {
	AreaID   uuid.UUID
	AreaName string
	Location []pip.Point
}

type Point struct {
	Latitude  float64
	Longitude float64
}

type ReadWriter interface {
	BulkInsertAddresses(ads []Address) error
	ReadAddressesNameByCity(city string) ([]string, error)
	ReadAdresssesName() ([]string, error)
	ReadOSMAddresses() ([]Address, error)
	ReadAreaIDByAreaName(areaName string) (uuid.UUID, error)
	ReadAddressLatLongFilterAddressLatLongNullByAreaID(areaID uuid.UUID) ([]AddressLatLong, error)
	ReadLatLongTrxLocHailing() ([]AddressLatLong, error)
	UpdateAreaIDCItyMstAddressByID(req Address) error
	ReadAreaIDNameLocsByAreaType(areaType int32) ([]Area, error)
	ReadIDLatLong() ([]Address, error)
	ReadLocationByAreaName(areaName string) ([]pip.Point, error)
}

type Filter interface {
	AddressExist(address string) bool
	InsertAddressFilter(address string)
	InsertMultipleAddressFilter(address []string)
	AddAreasForFilter(areas []Area)
	InsertArea(area Area)
	FilterAddresByArea(ads Address) Area
}

type MergingService interface {
	InsertMultipleData(ads []Address) error
	InsertMultipleDataByCSV(path string, cust CustomInsert) error
	InsertMultipleDataByTSV(path string, skip int) error
	GenerateOSMTSVFile(nameFile string) error
	InsertMultipleDataByJson(path string) error
	MigrateAddressTrxLocationToMstAddressByAreaName(areaName string) error
	MigrateAddressTrxLocationHailing() error
	CreateFileTrxLocationHailing() error
	GetReverseGeoData(latitude, longitude float64) (ReverseGeoResp, error)
	CreateReverseGeoJSONByTrxLocationJSON(nameJSON string) error
	FixingAreaIDCityALLAddress() error
}

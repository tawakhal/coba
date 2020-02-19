package server

import (
	"fmt"
	"log"
	"os"

	tsv "github.com/valyala/tsvreader"
)

var (
	DefaultHeaderOSMTSV OsmTsv = OsmTsv{
		name:              "name",
		alternative_names: "alternative_names",
		osm_type:          "osm_type",
		osm_id:            "osm_id",
		class:             "class",
		Type:              "Type",
		lon:               "lon",
		lat:               "lat",
		place_rank:        "place_rank",
		importance:        "importance",
		street:            "street",
		city:              "city",
		county:            "county",
		state:             "state",
		country:           "country",
		country_code:      "country_code",
		display_name:      "display_name",
		west:              "west",
		south:             "south",
		east:              "east",
		north:             "north",
		wikidata:          "wikidata",
		wikipedia:         "wikipedia",
		housenumbers:      "housenumbers",
	}
)

func ReadTSV(pathFile string, skip int) ([]OsmTsv, error) {
	// Open file
	file, err := os.Open(pathFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	i := 1

	var kakak []OsmTsv
	r := tsv.New(file)
	for r.Next() {
		dk := OsmTsv{
			name:              r.String(),
			alternative_names: r.String(),
			osm_type:          r.String(),
			osm_id:            r.String(),
			class:             r.String(),
			Type:              r.String(),
			lon:               r.String(),
			lat:               r.String(),
			place_rank:        r.String(),
			importance:        r.String(),
			street:            r.String(),
			city:              r.String(),
			county:            r.String(),
			state:             r.String(),
			country:           r.String(),
			country_code:      r.String(),
			display_name:      r.String(),
			west:              r.String(),
			south:             r.String(),
			east:              r.String(),
			north:             r.String(),
			wikidata:          r.String(),
			wikipedia:         r.String(),
			housenumbers:      r.String(),
		}
		if r.Error() != nil {
			log.Printf("%d:%v\n", i, r.Error())
			continue
		}

		for sd := 0; sd < skip; sd++ {
			r.SkipCol()
		}

		kakak = append(kakak, dk)
		i++
	}
	return kakak, nil
}

func createTSVFile(nameFile string, dt []OsmTsv) error {
	file, err := os.Create(nameFile + ".tsv")
	if err != nil {
		return err
	}
	defer file.Close()

	var allData []OsmTsv
	allData = append(allData, DefaultHeaderOSMTSV)
	allData = append(allData, dt...)

	k := 1
	tab := "\t"
	enter := "\n"
	for i, v := range allData {
		err := writeToFile(file, fmt.Sprintf("%s%s", v.name, tab))
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, fmt.Sprintf("%s%s", v.alternative_names, tab))
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.osm_type+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}

		err = writeToFile(file, v.osm_id+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}

		err = writeToFile(file, v.class+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.Type+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.lon+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.lat+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.place_rank+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.importance+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, fmt.Sprintf("%s%s", v.street, tab))
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.city+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.county+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.state+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.country+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.country_code+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, fmt.Sprintf("%s%s", v.display_name, tab))
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.west+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.south+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.east+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.north+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.wikidata+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.wikipedia+tab)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}
		err = writeToFile(file, v.housenumbers+enter)
		if err != nil {
			Logger.Log("[ERROR]", err.Error())
			break
		}

		if i+1 == 10000*k || i+1 == len(allData) {
			Logger.Log("[INFO]", fmt.Sprintf("Success for-%d", i+1))
			k++
		}
	}

	return nil
}

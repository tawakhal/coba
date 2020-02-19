package server

import (
	"encoding/csv"
	"os"
)

func ReadCSV(pathFile string) ([][]string, error) {
	// Open file
	file, err := os.Open(pathFile)
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return nil, err
	}
	defer file.Close()

	rwt, err := csv.NewReader(file).ReadAll()
	if err != nil {
		Logger.Log("[ERROR]", err.Error())
		return nil, err
	}
	return rwt, nil
}

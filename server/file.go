package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

func writeToFile(file *os.File, s string) error {
	_, err := file.WriteString(s)
	return err
}

func writeJSONFile(nameFile string, data interface{}) error {

	dataJSON, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fmt.Sprintf("%s.json", nameFile), dataJSON, 0755)
	if err != nil {
		return err
	}
	return nil
}

func readJSONFile(nameFile string) (*os.File, error) {
	opFile, err := os.Open(nameFile)
	if err != nil {
		return nil, err
	}
	return opFile, nil
}

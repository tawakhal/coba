package server

import "strings"

func cleanDrityWord(wrd string) string {
	for _, v := range dirtyWord {
		wrd = strings.Replace(wrd, v, "", -1)
	}
	return wrd
}

func completeAddress(address, city, country string) string {
	ad := strings.ToLower(address)
	if city != "" {
		if !strings.Contains(ad, city) {
			address += "," + city
		}
	}
	if country != "" {
		if !strings.Contains(ad, country) {
			address += "," + country
		}
	}
	return address
}

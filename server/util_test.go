package server

import "testing"

func TestCleanDrityWord(t *testing.T) {
	t.Fail()

	str := "<< orang - disana + ada #barang >>"
	str = cleanDrityWord(str)
	t.Log(str)
}

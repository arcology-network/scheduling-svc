package types

import (
	"reflect"
	"testing"
)

func TestFilter(t *testing.T) {
	keys := [][]string{{"12", "34"}, {"56", "78"}, {"90", "11"}}

	cl := NewCheckList(2)
	cl.OnReceivedList(keys[0])

	exist := cl.Exist("12")
	if !reflect.DeepEqual(exist, true) {
		t.Error("12 is not in checklist !", exist)
	}

	exist = cl.Exist("56")
	if !reflect.DeepEqual(exist, false) {
		t.Error("56 is in checklist !", exist)
	}

	cl.OnReceivedList(keys[1])

	exist = cl.Exist("56")
	if !reflect.DeepEqual(exist, true) {
		t.Error("56 is not in checklist !", exist)
	}

	cl.OnReceivedList(keys[2])

	exist = cl.Exist("90")
	if !reflect.DeepEqual(exist, true) {
		t.Error("90 is not in checklist !", exist)
	}

	exist = cl.Exist("12")
	if !reflect.DeepEqual(exist, false) {
		t.Error("12 is  in checklist !", exist)
	}
}

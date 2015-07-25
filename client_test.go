package themis

import "testing"

func TestNewClient(t *testing.T) {
	_, err := NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		t.Error(err)
	}
}

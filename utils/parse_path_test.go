package utils

import "testing"

func TestParseSSTPath(t *testing.T) {
	level, seq, err := ParseSSTPath("data/sst/0_1.sst")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(level, seq)
}
func TestParseWalPath(t *testing.T) {
	fileId, err := ParseWalPath("data/wal/0.wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(fileId)
}

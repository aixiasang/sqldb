package config

import "testing"

func TestConfig(t *testing.T) {
	conf := NewConfig()
	t.Log(conf)

}

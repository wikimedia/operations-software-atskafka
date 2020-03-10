package main

import "testing"

func TestLogLineToJsonOK(t *testing.T) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org"
	const j = `{"hostname":"cp3050.esams.wmnet","http_method":"GET","time_firstbyte":1235,"uri_host":"en.wikipedia.org"}`

	m := map[string]bool{"time_firstbyte": true}
	r, err := logLineToJson(l, m)

	if err != nil {
		t.Errorf("Expecting err to be nil, got %v instead", err)
	}

	if string(r) != j {
		t.Errorf("Got r=%s instead of j=%s", r, j)
	}
}

func TestLogLineToJsonKO(t *testing.T) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:notanumber"

	m := map[string]bool{"time_firstbyte": true}
	r, err := logLineToJson(l, m)

	if err == nil {
		t.Error("Expecting err to be set, got nil instead")
	}

	if r != nil {
		t.Errorf("Expecting r to be nil, got %v instead", r)
	}
}

func BenchmarkLogLineToJson(b *testing.B) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org"
	m := map[string]bool{"time_firstbyte": true}

	for i := 0; i < b.N; i++ {
		logLineToJson(l, m)
	}
}

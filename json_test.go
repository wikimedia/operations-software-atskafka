// Copyright (C) 2020 Emanuele Rocca <ema@wikimedia.org>
// Copyright (C) 2020 Wikimedia Foundation, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import "testing"

func TestLogLineToJsonOK(t *testing.T) {
	expected := map[string]string{
		"hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org	uri_path:/w/load.php?lang=en&modules=jquery%2Coojs-ui-core%2Coojs-ui-widgets&skin=vector&version=1wn7i": `{"hostname":"cp3050.esams.wmnet","http_method":"GET","time_firstbyte":1235,"uri_host":"en.wikipedia.org","uri_path":"/w/load.php","uri_query":"lang=en\u0026modules=jquery%2Coojs-ui-core%2Coojs-ui-widgets\u0026skin=vector\u0026version=1wn7i"}`,
		"hostname:cp3050.esams.wmnet	time_firstbyte:42	http_method:HEAD	uri_host:en.wikipedia.org	uri_path:/wiki/Main_Page": `{"hostname":"cp3050.esams.wmnet","http_method":"HEAD","time_firstbyte":42,"uri_host":"en.wikipedia.org","uri_path":"/wiki/Main_Page","uri_query":""}`,
	}

	for l, j := range expected {
		m := map[string]bool{"time_firstbyte": true}
		r, err := logLineToJson(l, m)

		if err != nil {
			t.Errorf("Expecting err to be nil, got %v instead", err)
		}

		if string(r) != j {
			t.Errorf("Got r=%s instead of j=%s", r, j)
		}
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

func BenchmarkLogLineToJsonNoQuery(b *testing.B) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org	uri_path:/wiki/Main_Page"
	m := map[string]bool{"time_firstbyte": true}

	for i := 0; i < b.N; i++ {
		logLineToJson(l, m)
	}
}

func BenchmarkLogLineToJsonYesQuery(b *testing.B) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org	uri_path:/w/load.php?lang=en"
	m := map[string]bool{"time_firstbyte": true}

	for i := 0; i < b.N; i++ {
		logLineToJson(l, m)
	}
}

func TestLoadConfig(t *testing.T) {
	conf := loadConfig("testdata/atskafka.conf")
	value, err := conf.Get("client.id", "")

	if err != nil {
		t.Errorf("Expecting err to be nil, got %v instead", err)
	}

	if value != "atskafka" {
		t.Errorf("Expecting client.id to be atskafka, got %v instead", value)
	}

	value, err = conf.Get("statistics.interval.ms", 0)

	if err != nil {
		t.Errorf("Expecting err to be nil, got %v instead", err)
	}

	if value != 60000 {
		t.Errorf("Expecting statistics.interval.ms to be 60000, got %v instead", value)
	}
}

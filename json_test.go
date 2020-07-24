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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogLineToJsonOK(t *testing.T) {
	input := []string{
		"hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org	uri_path:/w/load.php?lang=en&modules=jquery%2Coojs-ui-core%2Coojs-ui-widgets&skin=vector&version=1wn7i",
		"hostname:cp3050.esams.wmnet	time_firstbyte:42	http_method:HEAD	uri_host:en.wikipedia.org	uri_path:/wiki/Main_Page",
	}

	expected := []string{
		`{"hostname":"cp3050.esams.wmnet","http_method":"GET","time_firstbyte":1235,"uri_host":"en.wikipedia.org","uri_path":"/w/load.php","uri_query":"lang=en\u0026modules=jquery%2Coojs-ui-core%2Coojs-ui-widgets\u0026skin=vector\u0026version=1wn7i"}`,
		`{"hostname":"cp3050.esams.wmnet","http_method":"HEAD","time_firstbyte":42,"uri_host":"en.wikipedia.org","uri_path":"/wiki/Main_Page","uri_query":""}`,
	}

	for i, val := range input {
		m := map[string]bool{"time_firstbyte": true}
		r, err := logLineToJson(val, m)

		assert.EqualValues(t, expected[i], r, "JSON output mismatch")
		assert.NoError(t, err)
	}
}

func TestLogLineToJsonKO(t *testing.T) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:notanumber"

	m := map[string]bool{"time_firstbyte": true}
	r, err := logLineToJson(l, m)

	assert.Nil(t, r)
	assert.Error(t, err)
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

	assert.Equal(t, value, "atskafka")
	assert.Nil(t, err)

	value, err = conf.Get("statistics.interval.ms", 0)

	assert.Equal(t, value, 60000)
	assert.Nil(t, err)
}

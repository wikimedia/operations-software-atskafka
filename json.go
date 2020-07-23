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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func logLineToJson(line string, numericFields map[string]bool) ([]byte, error) {
	data := map[string]interface{}{}

	fields := strings.Split(line, "\t")

	for i := 0; i < len(fields); i++ {
		values := strings.SplitN(fields[i], ":", 2)
		if len(values) != 2 {
			return nil, fmt.Errorf("Cannot split '%s' into 'field:value'", fields[i])
		}

		key := values[0]
		value := values[1]

		if !numericFields[key] {
			if key == "uri_path" {
				// Extract uri_query, if any. For instance, turn
				// uri_path:/w/load.php?q=x into uri_path:/w/load.php and
				// uri_query:q=x.
				// Doing this out of the for loop might possibly look better
				// but is a bit slower due to the required type assertion to
				// convert from interface{}{} to string.
				uriPathQuery := strings.SplitN(value, "?", 2)
				data["uri_path"] = uriPathQuery[0]

				if len(uriPathQuery) == 2 {
					data["uri_query"] = uriPathQuery[1]
				} else {
					// varnishkafka defaults to empty string rather than null,
					// keep the behavior here too
					data["uri_query"] = ""
				}
			} else {
				data[key] = value
			}
		} else {
			num, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			} else {
				data[key] = num
			}
		}
	}

	j, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return j, nil
}

func loadConfig(f string) *kafka.ConfigMap {
	var vals kafka.ConfigMap
	jsonConfig, err := ioutil.ReadFile(f)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(jsonConfig, &vals)
	if err != nil {
		log.Fatal(err)
	}

	// Convert float64 values to int. When unmarshaling into an interface
	// value, json.Unmarshal uses float64 for numbers, while kafka.ConfigMap
	// expects integers.
	for k := range vals {
		if _, ok := vals[k].(float64); ok {
			vals[k] = int(vals[k].(float64))
		}
	}

	return &vals
}

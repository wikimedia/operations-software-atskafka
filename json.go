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
			data[key] = value
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

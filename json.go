package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/qri-io/jsonschema"
)

func loadSchema() *jsonschema.RootSchema {
	var s *jsonschema.RootSchema

	if *schemaPathFlag == "" {
		log.Println("No JSON schema provided, will perform simple validation")
		return nil
	}

	s = &jsonschema.RootSchema{}
	schemaData, err := ioutil.ReadFile(*schemaPathFlag)
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(schemaData, s); err != nil {
		log.Fatal(err)
	}

	return s
}

func logLineToJson(line string, numericFields map[string]bool) ([]byte, error) {
	data := map[string]interface{}{}

	fields := strings.Split(line, "\t")

	for i := 0; i < len(fields); i++ {
		values := strings.SplitN(fields[i], ":", 2)
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

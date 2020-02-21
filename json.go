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

func validJSON(str string, schema *jsonschema.RootSchema) bool {
	// Simple check for JSON validity
	if schema == nil {
		var js json.RawMessage
		return json.Unmarshal([]byte(str), &js) == nil
	}

	// Full schema validation
	errors, err := schema.ValidateBytes([]byte(str))
	if err != nil {
		return false
	}
	return len(errors) == 0
}

func logLineToJson(line string, numericFields map[string]bool) string {
	var msg strings.Builder
	msg.WriteString("{")

	fields := strings.Split(line, "\t")

	for i := 0; i < len(fields); i++ {
		values := strings.SplitN(fields[i], ":", 2)
		key := values[0]
		value := values[1]

		msg.WriteRune('"')
		msg.WriteString(key)
		msg.WriteString("\": ")

		if !numericFields[key] {
			msg.WriteString(strconv.Quote(value))
		} else {
			msg.WriteString(value)
		}

		if i == len(fields)-1 {
			msg.WriteRune('}')
		} else {
			msg.WriteString(", ")
		}
	}

	return msg.String()
}

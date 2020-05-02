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
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MockProducer struct {
	outChan chan *kafka.Message
}

func (p *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	p.outChan <- msg
	return nil
}

func TestDoWork(t *testing.T) {
	logChan := make(chan string, 10)

	p := MockProducer{outChan: make(chan *kafka.Message)}

	logChan <- ""
	logChan <- "Not what we would expect"
	logChan <- "hostname:cp3050.esams.wmnet\ttime_firstbyte:9\tcache_status:hit-front"

	go doWork(logChan, 0, &p)

	msg := <-p.outChan

	if *msg.TopicPartition.Topic != "test_topic" {
		t.Errorf("Unexpected topic: %s", *msg.TopicPartition.Topic)
	}

	type Data struct {
		Hostname      string `json:"hostname"`
		TimeFirstbyte int    `json:"time_firstbyte"`
		CacheStatus   string `json:"cache_status"`
	}

	var d Data

	err := json.Unmarshal(msg.Value, &d)
	if err != nil {
		t.Errorf("Error Unmarshaling JSON")
	}

	if d.Hostname != "cp3050.esams.wmnet" {
		t.Errorf("Unexpected value: %s", d.Hostname)
	}

	if d.TimeFirstbyte != 9 {
		t.Errorf("Unexpected value: %d", d.TimeFirstbyte)
	}

	if d.CacheStatus != "hit-front" {
		t.Errorf("Unexpected value: %s", d.CacheStatus)
	}
}

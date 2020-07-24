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
	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, "test_topic", *msg.TopicPartition.Topic, "Unexpected topic")

	type Data struct {
		Hostname      string `json:"hostname"`
		TimeFirstbyte int    `json:"time_firstbyte"`
		CacheStatus   string `json:"cache_status"`
	}

	var d Data

	err := json.Unmarshal(msg.Value, &d)
	assert.Nil(t, err)

	assert.Equal(t, "cp3050.esams.wmnet", d.Hostname, "Unexpected hostname")
	assert.Equal(t, 9, d.TimeFirstbyte, "Unexpected TTFB")
	assert.Equal(t, "hit-front", d.CacheStatus, "Unexpected cache-status")
}

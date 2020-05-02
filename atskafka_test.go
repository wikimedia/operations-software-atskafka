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

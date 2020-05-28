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
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"gerrit.wikimedia.org/r/operations/software/prometheus-rdkafka-exporter/promrdkafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
}

const (
	connectTimeout     = 1000 * time.Millisecond
	maxConnectAttempts = 16
)

var (
	connectAttempt    = 0
	socketPathFlag    = flag.String("socket", "/var/run/log.socket", "socket to communicate with fifo-log-demux")
	numericFieldsFlag = flag.String("numericFields", "response_size,sequence,time_firstbyte", "List of fields to be considered numeric")
	kafkaConfigFile   = flag.String("kafkaConfig", "/etc/atskafka.conf", "Kafka configuration file")
	kafkaTopicFlag    = flag.String("kafkaTopic", "test_topic", "Kafka topic")
	addr              = flag.String("addr", ":2113", "TCP network address for Prometheus and pprof endpoints")
	validLogsRegex    = flag.String("validLogsRegex", "http_status:[1-9]", "Regular expression to match logs against")
	deliveryErrors    = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "atskafka_delivery_errors_total",
			Help: "Total number of Kafka delivery errors",
		})
	seqNumber = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "atskafka_seq_number",
			Help: "Latest sequence number",
		})
)

func reader(c chan string) error {
	// Connect to fifo-log-demux socket
	conn, err := net.DialTimeout("unix", *socketPathFlag, connectTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send fifo-log-demux a regular expression to filter valid logs
	_, err = conn.Write([]byte(*validLogsRegex))
	if err != nil {
		return err
	}

	log.Println("Connected to", *socketPathFlag)
	connectAttempt = 0

	// Read lines from socket
	scanner := bufio.NewScanner(conn)
	seq := 0
	for scanner.Scan() {
		c <- fmt.Sprintf("sequence:%d\t%s", seq, scanner.Text())
		seq++
		// Update atskafka_seq_number
		seqNumber.Set(float64(seq))
	}
	return nil
}

func doWork(c chan string, i int, p KafkaProducer) {
	// List of fields not to be considered strings (eg: response_size)
	numericFields := make(map[string]bool)
	for _, field := range strings.Split(*numericFieldsFlag, ",") {
		numericFields[field] = true
	}

	for {
		line := <-c

		json, err := logLineToJson(line, numericFields)
		if err == nil {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: kafkaTopicFlag, Partition: kafka.PartitionAny},
				Value:          json,
			}, nil)
		} else {
			log.Printf("Error converting '%v' to JSON: %v\n", line, err)
		}
	}
}

// Delivery report handler for produced messages
func deliveryReport(p *kafka.Producer, m *promrdkafka.Metrics) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				deliveryErrors.Inc()
			}
		case *kafka.Stats:
			err := m.Update(e.String())
			if err != nil {
				log.Printf("Error updating stats: %v\n", err)
			}
		}
	}
}

func main() {
	flag.Parse()
	c := make(chan string)

	p, err := kafka.NewProducer(loadConfig(*kafkaConfigFile))

	if err != nil {
		log.Fatal("Fatal error: ", err)
	}
	defer p.Close()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt)
	go func() {
		for sig := range sc {
			log.Printf("captured %v, exiting..", sig)
			os.Exit(0)
		}
	}()

	m := promrdkafka.NewMetrics()
	go deliveryReport(p, m)

	// Serve metrics under /metrics, profiling info under /debug/pprof
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(*addr, nil)
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		go doWork(c, i, p)
	}

	for connectAttempt = 0; connectAttempt < maxConnectAttempts; connectAttempt++ {
		// By keeping this in a loop, atskafka is able to re-open the UNIX
		// socket after an error
		err := reader(c)

		connectRetryTime := math.Pow(2, float64(connectAttempt))
		log.Printf("Unable to read from socket: %v. Reconnecting in %v milliseconds.\n", err, connectRetryTime)
		time.Sleep(time.Duration(connectRetryTime) * time.Millisecond)
	}

	log.Fatal("Giving up trying to connect to ", *socketPathFlag)
}
